package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.conf.InvalidObject;
import org.apache.bookkeeper.conf.ServerTester;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import static java.lang.Thread.sleep;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class TestReadEntry {

    //Parameterized params
    private static long ledgerId;
    private static long entryId;
    private BookkeeperInternalCallbacks.ReadEntryCallback callback;
    private Object context;
    private static int flags;
    private static byte[] masterKey;
    private static boolean allowFastFail;

    //Init instances
    private static ServerTester server;
    private static PerChannelBookieClient bookieClient;
    private static boolean errorResultFromWriteCallback = false;
    private static boolean errorResultFromReadCallback = false;
    private static String resultFromReadCallback = "";


    private enum ParamType {
        NULL, EMPTY, VALID, INVALID
    }

    public TestReadEntry(long ledgerId, long entryId, ParamType callbackType, ParamType contextType,
                         int flags, ParamType masterKeyType, boolean allowFastFail) throws IOException, BookieException {

        configure(ledgerId, entryId, callbackType, contextType, flags, masterKeyType, allowFastFail);

    }

    private void configure(long ledgerId, long entryId, ParamType callbackType, ParamType contextType,
                           int flags, ParamType masterKeyType, boolean allowFastFail) throws IOException, BookieException {

        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.flags = flags;
        this.allowFastFail = allowFastFail;

        switch (callbackType) {
            case NULL:
                this.callback = null;
                break;
            case VALID:
//                this.callback = getMockedReadCb(false);
                this.callback = new BookkeeperInternalCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(int rc, long ledgerId, long entryId,
                                                  ByteBuf buffer, Object ctx) {
//                        if(buffer!=
                    }
                };
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

        switch (masterKeyType) {
            case NULL:
                this.masterKey = null;
                break;
            case EMPTY:
                this.masterKey = new byte[]{};
                break;
            case VALID:
                this.masterKey = "someMasterKey".getBytes(StandardCharsets.UTF_8);
                break;
            case INVALID:
                this.masterKey = "someInvalidMasterKey".getBytes(StandardCharsets.UTF_8);
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
            //ledgerID,entryID,callback,context,flags,masterKey,allowFastFail
            {0, 0, ParamType.NULL, ParamType.NULL, 0, ParamType.NULL, false},
            {0, 1, ParamType.NULL, ParamType.NULL, 1, ParamType.NULL, false},
            {1, 0, ParamType.NULL, ParamType.NULL, 2, ParamType.EMPTY, false},
            {1, 1, ParamType.INVALID, ParamType.NULL, 3, ParamType.INVALID, false},
            {0, 0, ParamType.INVALID, ParamType.NULL, 4, ParamType.INVALID, false},
            {0, 1, ParamType.INVALID, ParamType.NULL, 5, ParamType.VALID, true},
            {1, 0, ParamType.VALID, ParamType.NULL, 6, ParamType.NULL, true},
            {1, 1, ParamType.VALID, ParamType.NULL, 7, ParamType.EMPTY, true},
            {0, 0, ParamType.VALID, ParamType.NULL, 7, ParamType.EMPTY, true},
            {0, 1, ParamType.NULL, ParamType.INVALID, 6, ParamType.INVALID, true},
            {1, 0, ParamType.NULL, ParamType.INVALID, 5, ParamType.VALID, false},
            {1, 1, ParamType.NULL, ParamType.INVALID, 4, ParamType.VALID, false},
            {0, 0, ParamType.INVALID, ParamType.INVALID, 3, ParamType.NULL, false},
            {0, 1, ParamType.INVALID, ParamType.INVALID, 2, ParamType.NULL, false},
            {1, 0, ParamType.INVALID, ParamType.INVALID, 1, ParamType.EMPTY, false},
            {1, 1, ParamType.VALID, ParamType.INVALID, 0, ParamType.INVALID, true},
            {0, 0, ParamType.VALID, ParamType.INVALID, 0, ParamType.INVALID, true},
            {0, 1, ParamType.VALID, ParamType.INVALID, 1, ParamType.VALID, true},
            {1, 0, ParamType.NULL, ParamType.VALID, 2, ParamType.NULL, true},
            {1, 1, ParamType.NULL, ParamType.VALID, 3, ParamType.NULL, true},
            {0, 0, ParamType.NULL, ParamType.VALID, 4, ParamType.EMPTY, false},
            {0, 1, ParamType.INVALID, ParamType.VALID, 5, ParamType.INVALID, false},
            {1, 0, ParamType.INVALID, ParamType.VALID, 6, ParamType.INVALID, false},
            {1, 1, ParamType.INVALID, ParamType.VALID, 7, ParamType.VALID, false},
            {0, 0, ParamType.VALID, ParamType.VALID, 7, ParamType.NULL, false},
            {0, 1, ParamType.VALID, ParamType.VALID, 6, ParamType.EMPTY, true},
            {1, 0, ParamType.VALID, ParamType.VALID, 5, ParamType.INVALID, true},
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
                System.out.println("In writecomplete");
                if (rc != 0) { //rc=0 indica che non c'è stato alcun errore
                    errorResultFromWriteCallback = true;
                }
                System.out.println(rc);
                System.out.println(errorResultFromWriteCallback);
            }
        };
        return cb;
    }

    private static PerChannelBookieClient getPerChannelBookieClient(ServerConfiguration conf) throws Exception {

        server = new ServerTester(conf);
        server.getServer().getBookie().getLedgerStorage().setMasterKey(ledgerId,
                "someMasterKey".getBytes(StandardCharsets.UTF_8));
        server.getServer().start();
        return new PerChannelBookieClient(OrderedExecutor.newBuilder().build(), new NioEventLoopGroup(),
                server.getServer().getBookieId(), BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

    }

    @BeforeClass
    public static void setup(){
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

    private boolean getExceptionExpected(){
        if (((this.flags & 1) == 1 && this.masterKey == null && this.callback == null)) return true;
        else return false;
    }

    @Test
    public void testReadEntry() {

        boolean isExceptionExpected = getExceptionExpected();

        try {
            this.bookieClient.readEntry(this.ledgerId, this.entryId, this.callback, this.context,
                    this.flags, this.masterKey, this.allowFastFail);

            Assert.assertFalse("An exception was expected.", isExceptionExpected);

        } catch (Exception e) {
            System.out.println("Exception catched.");
            Assert.assertTrue("We expected no exception, but " + e.getClass().getName(),
                    isExceptionExpected);
        }

    }


//    @AfterClass
//    public static void tearDown() {
//        server.getServer().getBookie().shutdown();
//
//    }


}