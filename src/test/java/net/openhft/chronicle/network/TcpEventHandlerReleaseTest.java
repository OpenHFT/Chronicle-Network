package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.BytesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TcpEventHandlerReleaseTest {
    private static final String hostPort = "host.port";

    @After
    public void teardown() {
        TCPRegistry.reset();
    }

    @Before
    public void setUp() throws IOException {
        TCPRegistry.createServerSocketChannelFor(hostPort);
    }

    @Test
    public void testRelease() throws IOException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        TcpEventHandler t = new TcpEventHandler(nc);
        t.close();
        // check second close OK
        t.close();
        BytesUtil.checkRegisteredBytes();
    }
}
