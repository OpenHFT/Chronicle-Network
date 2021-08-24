package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class TcpEventHandlerReleaseTest extends NetworkTestCommon {
    private static final String hostPort = "host.port";

    @Before
    public void setUp() throws IOException {
        TCPRegistry.createServerSocketChannelFor(hostPort);
    }

    @Test
    public void testRelease() throws IOException {
        try (TcpEventHandler t = createTcpEventHandler()) {
            t.loopFinished();
            t.close();
            // check second close OK
        }
    }

    @Test
    public void testBuffersReleasedWhenSocketChannelClosed() throws IOException {
        try (TcpEventHandler t = createTcpEventHandler()) {
            t.socketChannel().close();
            try {
                t.action();
                fail();
            } catch (InvalidEventHandlerException e) {
                // expected.
            } finally {
                t.loopFinished();
            }
        }
    }

    @Test
    public void performIdleWorkIsOnlyCalledWhenHandlerIsBusyOrOneHundredIterations() throws IOException, InvalidEventHandlerException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        BusyTcpEventHandler tcpEventHandler = new BusyTcpEventHandler(nc);
        final BusyTcpHandler tcpHandler = new BusyTcpHandler();
        tcpEventHandler.tcpHandler(tcpHandler);

        // not called when busy
        tcpEventHandler.busy = true;
        tcpEventHandler.action();
        assertEquals(0, tcpHandler.performedIdleWorkCount.get());

        // called when not busy
        tcpEventHandler.busy = false;
        tcpEventHandler.action();
        assertEquals(1, tcpHandler.performedIdleWorkCount.get());

        // called when not called for 101 iterations
        tcpEventHandler.busy = true;
        for (int i = 0; i < 101; i++) {
            tcpEventHandler.action();
        }
        assertEquals(1, tcpHandler.performedIdleWorkCount.get());
        tcpEventHandler.action();
        assertEquals(2, tcpHandler.performedIdleWorkCount.get());
        tcpEventHandler.close();
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

    /**
     * This is nasty, but the TcpEventHandler is very hard to test
     */
    private static class BusyTcpEventHandler extends TcpEventHandler {

        private boolean busy = true;

        public BusyTcpEventHandler(@NotNull NetworkContext nc) {
            super(nc, true);
        }

        @Override
        public boolean writeAction() {
            return busy;
        }
    }

    private static class BusyTcpHandler implements TcpHandler {

        private final AtomicInteger performedIdleWorkCount = new AtomicInteger();

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

        @Override
        public void performIdleWork() {
            performedIdleWorkCount.incrementAndGet();
        }
    }
}
