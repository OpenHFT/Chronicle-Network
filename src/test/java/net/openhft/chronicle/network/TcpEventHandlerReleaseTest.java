package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.fail;

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
        try {
            BytesUtil.checkRegisteredBytes();
        } catch (Throwable t) {
            // just doing this to reset BytesUtil. TODO: fix other tests to not leak Bytes
        }
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        TcpEventHandler t = new TcpEventHandler(nc);
        t.close();
        // assume the handle has been added to an EventLoop.
        try {
            t.action();
            fail();
        } catch (InvalidEventHandlerException e) {
            // expected.
        }
        // check second close OK
        t.close();
        BytesUtil.checkRegisteredBytes();
    }
}
