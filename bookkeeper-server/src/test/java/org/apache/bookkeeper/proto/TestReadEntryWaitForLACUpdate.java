package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.*;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

import static java.lang.Thread.sleep;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class TestReadEntryWaitForLACUpdate {

    //Parameterized params
    private static long ledgerId;
    private static long entryId;
    private BookkeeperInternalCallbacks.ReadEntryCallback callback;
    private Object context;
    private long previousLAC;
    private long timeOutInMillis;
    private boolean piggyBackEntry;

    private static boolean v2wireprotocol = false;

    //Init instances
    private static ServerTester server;
    private static PerChannelBookieClient bookieClient;

    private static boolean errorResultFromWriteCallback = false;
    private static boolean errorResultFromReadCallback = false;
    private static String resultFromReadCallback = "";


    private enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }

    public TestReadEntryWaitForLACUpdate(long ledgerId, long entryId, ParamType callbackType, ParamType contextType,
                                         long previousLAC, long timeOutInMillis, boolean piggyBackEntry, boolean v2wireprotocol) {

        configure(ledgerId, entryId, callbackType, contextType, previousLAC, timeOutInMillis, piggyBackEntry, v2wireprotocol);

    }

    private void configure(long ledgerId, long entryId, ParamType callbackType, ParamType contextType,
                           long previousLAC, long timeOutInMillis, boolean piggyBackEntry, boolean v2wireprotocol) {

        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.previousLAC = previousLAC;
        this.timeOutInMillis = timeOutInMillis;
        this.piggyBackEntry = piggyBackEntry;
        this.v2wireprotocol = v2wireprotocol;

        switch (callbackType) {
            case NULL:
                this.callback = null;
                break;
            case VALID:
                this.callback = getReadCb(false);
                break;
            case INVALID:
                this.callback = getReadCb(true);
                break;
        }

        switch (contextType) {
            case NULL:
                this.context = null;
                break;
            case VALID:
                this.context = new Object();
                break;
            case INVALID:
                this.context = new InvalidObject();
                break;
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //ledgerID,entryID,callback,context,previousLAC,timeOutInMillis,piggyBackEntry,v2wireprotocol
                {0, 0, ParamType.NULL, ParamType.VALID, -1, -1, true, false},
                {0, 1, ParamType.NULL, ParamType.VALID, -1, 0, true, true},
                {1, 0, ParamType.INVALID, ParamType.NULL, -1, 1, true, false},
                {1, 1, ParamType.INVALID, ParamType.INVALID, 0, -1, true, true},
                {0, 0, ParamType.VALID, ParamType.VALID, 0, 0, true, false},
                {0, 1, ParamType.VALID, ParamType.VALID, 0, 1, true, true},
                {1, 0, ParamType.NULL, ParamType.NULL, 1, -1, true, false},
                {1, 1, ParamType.NULL, ParamType.INVALID, 1, 0, true, true},
                {0, 0, ParamType.INVALID, ParamType.VALID, 1, 1, true, false},
                {0, 1, ParamType.INVALID, ParamType.VALID, -1, -1, false, true},
                {1, 0, ParamType.VALID, ParamType.VALID, -1, 0, false, false},
                {1, 1, ParamType.VALID, ParamType.NULL, -1, 1, false, true},
                {0, 0, ParamType.NULL, ParamType.VALID, 0, -1, false, false},
                {0, 1, ParamType.NULL, ParamType.INVALID, 0, 0, false, true},
                {1, 0, ParamType.INVALID, ParamType.VALID, 0, 1, false, false},
                {1, 1, ParamType.INVALID, ParamType.NULL, 1, -1, false, true},
                {0, 0, ParamType.VALID, ParamType.VALID, 1, 0, false, false},
                {0, 1, ParamType.VALID, ParamType.INVALID, 1, 1, false, true},
        });
    }


    private BookkeeperInternalCallbacks.ReadEntryCallback getReadCb(boolean invalid) {
        BookkeeperInternalCallbacks.ReadEntryCallback cb;

        if(invalid){
            cb = mock(BookkeeperInternalCallbacks.ReadEntryCallback.class);
            doThrow(Exception.class).when(cb).readEntryComplete(isA(Integer.class), isA(Long.class), isA(Long.class), isA(ByteBuf.class),
                    isA(Object.class));
        }else{
            cb = new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    System.out.println("In readcomplete");
                    if (rc == 0) { //rc=0 indica che non c'è stato alcun errore
                        while (buffer.isReadable()) {
                            resultFromReadCallback += (char) buffer.readByte();
                        }
                    } else {
                        errorResultFromReadCallback = true;
                    }
                }
            };
        }

        return cb;
    }


    private static BookkeeperInternalCallbacks.WriteCallback getWriteCb() {

        BookkeeperInternalCallbacks.WriteCallback cb = new BookkeeperInternalCallbacks.WriteCallback() {

            @Override
            public void writeComplete(int rc, long ledger, long entry, BookieId addr, Object ctx) {
                if (rc != 0) { //rc=0 indica che non c'è stato alcun errore
                    errorResultFromWriteCallback = true;
                }
            }
        };
        return cb;
    }

    private static PerChannelBookieClient getPerChannelBookieClient(ServerConfiguration conf) throws Exception {

        server = new ServerTester(conf);
        server.getServer().getBookie().getLedgerStorage().setMasterKey(ledgerId,
                "someMasterKey".getBytes(StandardCharsets.UTF_8));
        server.getServer().start();

        ClientConfiguration newConf = new ClientConfiguration();
        if (v2wireprotocol) newConf.setUseV2WireProtocol(true);

        return new PerChannelBookieClient(newConf, OrderedExecutor.newBuilder().build(), new NioEventLoopGroup(),
                server.getServer().getBookieId(), NullStatsLogger.INSTANCE, null, null,
                null, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

    }

    @Before
    public void setup(){
        try {
            bookieClient = getPerChannelBookieClient(TestBKConfiguration.newServerConfiguration());
        } catch (Exception e) {
            Assert.fail("Errore avvio server");
        }

        ByteBuf byteBuf = Unpooled.buffer("someEntry".getBytes(StandardCharsets.UTF_8).length);
        ByteBufList byteBufList = ByteBufList.get(byteBuf);
        EnumSet<WriteFlag> writeFlags = EnumSet.noneOf(WriteFlag.class);

        bookieClient.addEntry(0, "someMasterKey".getBytes(StandardCharsets.UTF_8), 0, byteBufList, getWriteCb(),
                new Object(), 0, false, writeFlags);

        // Ho provato ad aggiungere la entry, ma qualsiasi configurazione usi ottengo dal callback rc=-8 (BKBookieHandleNotAvailableException)
        // Per cui, i test effettuati riguardano unicamente il comportamento del metodo readEntry
    }

    private boolean getExceptionExpected() {
        if (Objects.isNull(previousLAC) && (piggyBackEntry || !Objects.isNull(timeOutInMillis)))
            return true;
        return false;
    }

    @Test
    public void testReadEntryWaitForLACUpdate() {
        boolean isExceptionExpected = getExceptionExpected();

        try {
            bookieClient.readEntryWaitForLACUpdate(ledgerId, this.entryId, this.previousLAC,
                    this.timeOutInMillis, this.piggyBackEntry, this.callback, this.context);

            Assert.assertFalse("An exception was expected.", isExceptionExpected);

        } catch (Exception e) {
            Assert.assertTrue("We expected no exception, but " + e.getClass().getName(),
                    isExceptionExpected);
        }

    }

    @After
    public void tearDown() {
        server.getServer().getBookie().shutdown();
    }


}


