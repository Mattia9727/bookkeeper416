package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.InvalidWriteSet;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.awaitility.Awaitility;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static org.apache.bookkeeper.client.DistributionSchedule.NULL_WRITE_SET;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class TestWaitForWritable{

    private DistributionSchedule.WriteSet writeSet;
    private int allowedNonWritableCount;
    private long durationMs;
    private boolean disableWrite;
    private boolean isException;

    private static BookKeeper bk_client;
    private static LocalBookKeeper lbk_client;
    private static LedgerHandle ledgerHandle;

    protected final ZooKeeperCluster zkUtil = new ZooKeeperUtil();

    public TestWaitForWritable(DistributionSchedule.WriteSet writeSet,
                               int allowedNonWritableCount, long durationMs, boolean disableWrite, boolean expectedResult) {
        configure(writeSet, allowedNonWritableCount, durationMs, disableWrite, expectedResult);
    }

    private void configure(DistributionSchedule.WriteSet writeSet,
                           int allowedNonWritableCount, long durationMs, boolean disableWrite, boolean isException){
        this.writeSet = writeSet;
        this.allowedNonWritableCount = allowedNonWritableCount;
        this.durationMs = durationMs;
        this.disableWrite = disableWrite;
        this.isException = isException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        DistributionSchedule.WriteSet ws = new RoundRobinDistributionSchedule(3,3,3).getWriteSet(123456789);
        InvalidWriteSet iws = new InvalidWriteSet();
        return Arrays.asList(new Object[][] {
                //writeSet, allowedNonWritableCount, durationMs, disableWrite, exception
                // Primi casi di test: category partition
                {NULL_WRITE_SET,-1,-1,false,false},
                {NULL_WRITE_SET,-1,0,false,false},
                {NULL_WRITE_SET,-1,1,false,false},
                {NULL_WRITE_SET,0,-1,false,false},
                {NULL_WRITE_SET,0,0,false,false},
                {NULL_WRITE_SET,0,1,false,false},
                {NULL_WRITE_SET,1,-1,false,false},
                {NULL_WRITE_SET,1,0,false,false},
                {NULL_WRITE_SET,1,1,false,false},
                {iws,-1,-1,false,true},
                {iws,-1,0,false,true},
                {iws,-1,1,false,true},
                {iws,0,-1,false,true},
                {iws,0,0,false,true},
                {iws,0,1,false,true},
                {iws,1,-1,false,true},
                {iws,1,0,false,true},
                {iws,1,1,false,true},
                {ws,-1,-1,false,false},
                {ws,-1,0,false,false},
                {ws,-1,1,false,false},
                {ws,0,-1,false,false},
                {ws,0,0,false,false},
                {ws,0,1,false,false},
                {ws,1,-1,false,false},
                {ws,1,0,false,false},
                {ws,1,1,false,false},
                //Seconda iterazione: aggiunti test con scrittura disabilitata
                {ws,-1,-1,true,false},
                {ws,-1,0,true,false},
                {ws,-1,1,true,false},
                {ws,0,-1,true,false},
                {ws,0,0,true,false},
                {ws,0,1,true,false},
                {ws,1,-1,true,false},
                {ws,1,0,true,false},
                {ws,1,1,true,false},
                //Terza iterazione: PIT e kill di mutazioni
        });
    }



    @BeforeClass
    public static void startupLocalBookkeeper() throws Exception {

//        try {
//            ServerConfiguration conf = new ServerConfiguration();
//            conf.setAllowLoopback(true);
//            lbk_client = LocalBookKeeper.getLocalBookies("127.0.0.1", 2182, 3, true, conf);
//            lbk_client.start();
//        } catch (Exception e) {
//            Assert.fail("Errore inizializzazione LocalBookKeeper");
//        }

        try {
            bk_client = new BookKeeper("127.0.0.1:2181");
        } catch (Exception e) {
            Assert.fail("Errore inizializzazione client BookKeeper");

        }
    }

    @Before
    public void defineNewLedger() throws BKException, InterruptedException {
        try {
            ledgerHandle = bk_client.createLedger(1, 1, 1, BookKeeper.DigestType.MAC, "somePassword".getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            Assert.fail("Errore creazione ledger");
        }
    }

    @After
    public void removeLedger() throws BKException, InterruptedException {
        bk_client.deleteLedger(1);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        bk_client.close();
    }

    private void setTargetChannelState(BookKeeper bkc, BookieId address,
                                       long key, boolean writable) throws Exception {
        ((BookieClientImpl) bkc.getBookieClient()).lookupClient(address).obtain((rc, pcbc) -> {
            pcbc.setWritable(writable);
        }, key);
    }

    @Test
    public void testWaitForWritable() throws Exception {
        Exception exception = null;

        try {
            if (writeSet != NULL_WRITE_SET) {
                int bookieIndex = writeSet.get(0);
                List<BookieId> curEns = ledgerHandle.getCurrentEnsemble();
                // disable channel writable
                setTargetChannelState(bk_client, curEns.get(bookieIndex), 0, !disableWrite);
            }
            boolean testResult = ledgerHandle.waitForWritable(writeSet, allowedNonWritableCount, durationMs);
            if (disableWrite && durationMs>=0)
                assertFalse(testResult);
            else
                assertTrue(testResult);
        } catch (Exception e){
            exception = e;
        }

        if(isException)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
        }
    }



    @Test
    public void testWaitForWritableThreaded() throws Exception {
        Exception exception = null;

        try {
            if (writeSet != NULL_WRITE_SET) {
                int bookieIndex = writeSet.get(0);

                List<BookieId> curEns = ledgerHandle.getCurrentEnsemble();

                // disable channel writable
                setTargetChannelState(bk_client, curEns.get(bookieIndex), 0, !disableWrite);
            }

            AtomicBoolean isWriteable = new AtomicBoolean(false);
            final long timeout = 2000;

            // waitForWritable async
            Thread wfwThread = new Thread(() -> isWriteable.set(ledgerHandle.waitForWritable(writeSet, allowedNonWritableCount, timeout)));
            wfwThread.start();
            sleep(1000);
            wfwThread.interrupt();

            if(disableWrite)
                Assert.assertFalse(isWriteable.get());
            else
                Assert.assertTrue(isWriteable.get());
        } catch (Exception e){
            exception = e;
        }

        if(isException)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
        }

//        Awaitility.await().pollDelay(5, TimeUnit.SECONDS).untilAsserted(() -> assertFalse(isWriteable.get()));

        // enable channel writable
//        setTargetChannelState(bk_client, curEns.get(slowBookieIndex), 0, true);
//        Awaitility.await().untilAsserted(() -> assertTrue(isWriteable.get()));

    }



}

//allowednonwritablecount=0
//durationMs = 1
