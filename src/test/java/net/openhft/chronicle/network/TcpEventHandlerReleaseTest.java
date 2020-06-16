package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.fail;

public class TcpEventHandlerReleaseTest extends NetworkTestCommon {
    private static final String hostPort = "host.port";

    @Before
    public void setUp() throws IOException {
        TCPRegistry.createServerSocketChannelFor(hostPort);
    }

    @Test
    public void testRelease() throws IOException {
        TcpEventHandler t = createTcpEventHandler();
        t.loopFinished();
        t.close();
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
        t.loopFinished();
        t.close();
    }

    public TcpEventHandler createTcpEventHandler() throws IOException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        TcpEventHandler tcpEventHandler = new TcpEventHandler(nc);
        tcpEventHandler.tcpHandler(NullTcpHandler.INSTANCE);
        return tcpEventHandler;
    }

    enum NullTcpHandler implements TcpHandler {
        INSTANCE;

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, NetworkContext nc) {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }
}
