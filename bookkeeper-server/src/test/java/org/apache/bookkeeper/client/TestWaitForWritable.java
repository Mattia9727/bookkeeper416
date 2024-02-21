package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.InvalidWriteSet;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.test.ZooKeeperUtil;
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
    private ParamType writeSetType;
    private int allowedNonWritableCount;
    private long durationMs;
    private boolean disableWrite;
    private boolean isException;

    private static BookKeeper bk_client;
    private static LocalBookKeeper lbk_client;
    private static LedgerHandle ledgerHandle;

    final byte[] entry = "Test Entry".getBytes();
    static long entryId;

    public TestWaitForWritable(ParamType writeSetType,
                               int allowedNonWritableCount, long durationMs, boolean disableWrite, boolean expectedResult) {
        configure(writeSetType, allowedNonWritableCount, durationMs, disableWrite, expectedResult);
    }

    private void configure(ParamType writeSetType,
                           int allowedNonWritableCount, long durationMs, boolean disableWrite, boolean isException){
        this.writeSetType = writeSetType;
        this.allowedNonWritableCount = allowedNonWritableCount;
        this.durationMs = durationMs;
        this.disableWrite = disableWrite;
        this.isException = isException;
    }

    public enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }


    @Parameterized.Parameters
    public static Collection<Object[]> data() {


        return Arrays.asList(new Object[][] {
                //writeSet, allowedNonWritableCount, durationMs, disableWrite, exception
                // Primi casi di test: category partition
                {ParamType.NULL,-1,-1,false,true},
                {ParamType.NULL,-1,0,false,true},
                {ParamType.NULL,-1,1,false,true},
                {ParamType.NULL,0,0,false,true},
                {ParamType.NULL,0,1,false,true},
                {ParamType.NULL,1,0,false,true},
                {ParamType.NULL,1,1,false,true},
                {ParamType.INVALID,-1,0,false,true},
                {ParamType.INVALID,-1,1,false,true},
                {ParamType.INVALID,0,0,false,true},
                {ParamType.INVALID,0,1,false,true},
                {ParamType.INVALID,1,0,false,true},
                {ParamType.INVALID,1,1,false,true},
                {ParamType.VALID,-1,0,false,false},
                {ParamType.VALID,-1,1,false,false},
                {ParamType.VALID,0,0,false,false},
                {ParamType.VALID,0,1,false,false},
                {ParamType.VALID,1,0,false,false},
                {ParamType.VALID,1,1,false,false},
                //Seconda iterazione: aggiunti test con scrittura disabilitata
                {ParamType.VALID,-1,0,true,false},
                {ParamType.VALID,-1,1,true,false},
                {ParamType.VALID,0,0,true,false},
                {ParamType.VALID,0,1,true,false},
                {ParamType.VALID,1,0,true,false},
                {ParamType.VALID,1,1,true,false},
                //Tempi di attesa più lunghi per trigger InterruptException
                {ParamType.VALID,-1,1000,true,false},
                {ParamType.VALID,0,1000,true,false},
                {ParamType.VALID,1,1000,true,false},
        });
    }



    @BeforeClass
    public static void startupLocalBookkeeper() throws Exception {

        boolean connected = false;


        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerConfiguration conf = new ServerConfiguration();
                    conf.setAllowLoopback(true);
                    lbk_client = LocalBookKeeper.getLocalBookies("127.0.0.1", 2181, 3, true, conf);
                    lbk_client.start();
                    System.out.println("________________________________________________________________________ LocalBookies OK _______________________________________________________________________________________");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // In locale, per velocizzare il setup dei test (soprattutto per PITest), il thread non viene lanciato.
        // Al suo posto, viene lanciata un'istanza di LocalBookkeeper (codice in LocalBookkeeperLauncher.java)
        thread.start();

        while (!connected) {
            try {
                Thread.sleep(10000);
                bk_client = new BookKeeper("127.0.0.1:2181");
                connected = true;

            } catch (Exception e) {
            }
        }

    }

    @Before
    public void defineNewLedger() throws BKException, InterruptedException {
        try {
            ledgerHandle = bk_client.createLedger(1, 1, 1, BookKeeper.DigestType.MAC, "somePassword".getBytes(StandardCharsets.UTF_8));
            switch(writeSetType) {
                case NULL:
                    writeSet = NULL_WRITE_SET;
                    break;
                case VALID:
                    entryId = ledgerHandle.addEntry(entry);
                    writeSet = new RoundRobinDistributionSchedule(1,1,1).getWriteSet(entryId);
                    break;
                case INVALID:
                    writeSet = new InvalidWriteSet();
                    break;
            }
        }catch (Exception e){
            Assert.fail("Errore creazione ledger/writeSet");
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

    private void setTargetChannelState(BookKeeper bkc, BookieId address, long key, boolean writable){
        ((BookieClientImpl) bkc.getBookieClient()).lookupClient(address).obtain((rc, pcbc) -> {
            pcbc.setWritable(writable);
        }, key);
    }

    @Test
    public void testWaitForWritableThreaded(){
        Exception exception = null;

        try {
            int bookieIndex = writeSet.get(0);
            List<BookieId> curEns = ledgerHandle.getCurrentEnsemble();
            // disable channel writable
            setTargetChannelState(bk_client, curEns.get(bookieIndex), 0, !disableWrite);

            boolean testResult = ledgerHandle.waitForWritable(writeSet, allowedNonWritableCount, durationMs);
            if (disableWrite && durationMs>=0)
                assertFalse(testResult);
            else
                assertTrue(testResult);

            //Seconda iterazione: coverage JaCoCo
            if (disableWrite && durationMs > 500){
                AtomicBoolean isWriteable = new AtomicBoolean(false);
                Thread wfwThread = new Thread(() -> isWriteable.set(ledgerHandle.waitForWritable(writeSet, allowedNonWritableCount, durationMs)));
                wfwThread.start();
                sleep(500);
                wfwThread.interrupt();

                if(disableWrite && durationMs>=0)
                    Assert.assertFalse(isWriteable.get());
                else
                    Assert.assertTrue(isWriteable.get());
            }

        } catch (Exception e){
            exception = e;
        }

        if(isException)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
        }
    }

//    //Terza iterazione: PIT e kill di mutazioni
    @Test
    public void testWaitForWritableThreadedWithPolling(){

        if (disableWrite && durationMs>=0) {

            int bookieIndex = writeSet.get(0);
            List<BookieId> curEns = ledgerHandle.getCurrentEnsemble();

            // disable channel writable
            setTargetChannelState(bk_client, curEns.get(bookieIndex), 0, false);

            AtomicBoolean isWriteable = new AtomicBoolean(false);
            final long timeout = 3000;

            // waitForWritable async
            new Thread(() -> isWriteable.set(ledgerHandle.waitForWritable(writeSet, allowedNonWritableCount, timeout))).start();

            Awaitility.await().timeout(2500, TimeUnit.MILLISECONDS).pollDelay(2, TimeUnit.SECONDS).untilAsserted(() -> assertFalse(isWriteable.get()));
            // enable channel writable
            setTargetChannelState(bk_client, curEns.get(bookieIndex), 0, true);
            Awaitility.await().timeout(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> assertTrue(isWriteable.get()));

        }
    }
}
