package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.Assert;

public class LocalBookkeeperLauncher {

    private LocalBookKeeper lbk_client;

    public LocalBookkeeperLauncher() {
        try {
            ServerConfiguration conf = new ServerConfiguration();
            conf.setAllowLoopback(true);
            lbk_client = LocalBookKeeper.getLocalBookies("127.0.0.1", 2181, 3, true, conf);
            lbk_client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        LocalBookkeeperLauncher lbk = new LocalBookkeeperLauncher();
    }

}
