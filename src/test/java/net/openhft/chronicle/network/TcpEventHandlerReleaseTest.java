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

    @After
    public void checkRegisteredBytes() {
        BytesUtil.checkRegisteredBytes();
    }

    @Test
    public void testRelease() throws IOException {
        TcpEventHandler t = createTcpEventHandler();
        t.close();
        // assume the handler has been added to an EventLoop.
        try {
            t.action();
            fail();
        } catch (InvalidEventHandlerException e) {
            // expected.
        }
        // check second close OK
        t.close();
    }

    @Test
    public void testBuffersReleasedWhenSocketChannelClosed() throws IOException {
        TcpEventHandler t = createTcpEventHandler();
        t.socketChannel().close();
        try {
            t.action();
            fail();
        } catch (InvalidEventHandlerException e) {
            // expected.
        }
        t.close();
    }

    public TcpEventHandler createTcpEventHandler() throws IOException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        TcpEventHandler tcpEventHandler = new TcpEventHandler(nc);
        tcpEventHandler.tcpHandler((in, out, nc1) -> { });
        return tcpEventHandler;
    }
}
