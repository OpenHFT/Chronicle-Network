package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.test.TestClusterContext;
import net.openhft.chronicle.network.test.TestClusteredNetworkContext;
import net.openhft.chronicle.testframework.Waiters;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.internal.util.io.IOUtil.closeQuietly;

class RemoteConnectorTest extends NetworkTestCommon {

    private RemoteConnector<TestClusteredNetworkContext> remoteConnector;
    private volatile Object connectedToSocketAddress;
    private AtomicBoolean tcpHandlerAddedToEventLoop = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        remoteConnector = new RemoteConnector<>(this::createTcpEventHandler);
    }

    @Test
    @Timeout(3)
    void remoteConnectorLooksUpAddressOnEachIteration() throws IOException {
        final ChronicleServerSocketChannel initialSocketChannel = TCPRegistry.createServerSocketChannelFor("test-server");
        Jvm.startup().on(RemoteConnectorTest.class, "Initial socket address is " + initialSocketChannel.socket().getLocalSocketAddress());
        try (final EventGroup eventGroup = EventGroup.builder().build();
             final TestClusterContext clusterContext = new TestClusterContext();
             final TestClusteredNetworkContext nc = new TestClusteredNetworkContext(clusterContext)) {
            eventGroup.start();
            closeQuietly(initialSocketChannel);
            // Will resolve initial channel, which is closed
            remoteConnector.connect("test-server", eventGroup, nc, 10);
            Jvm.pause(100);
            assertNull(connectedToSocketAddress);   // it never connected
            // Reassign name to a fail-over channel
            final ChronicleServerSocketChannel failOverChannel = TCPRegistry.createServerSocketChannelFor("test-server");
            assertNotEquals(failOverChannel.socket().getLocalPort(), initialSocketChannel.socket().getLocalPort());
            Jvm.startup().on(RemoteConnectorTest.class, "Failover socket address is " + failOverChannel.socket().getLocalSocketAddress());
            // Accept the connection
            try (final ChronicleSocketChannel accept = failOverChannel.accept()) {
                // Assert we connected to the fail-over socket
                assertNotNull(accept);
                Waiters.waitForCondition("Client connected address not populated", () -> tcpHandlerAddedToEventLoop.get(), 3_000);
                assertEquals(failOverChannel.socket().getLocalSocketAddress(), connectedToSocketAddress);
            }
        }
    }

    private TcpEventHandler<TestClusteredNetworkContext> createTcpEventHandler(TestClusteredNetworkContext nc) {
        connectedToSocketAddress = nc.socketChannel().socket().getRemoteSocketAddress();
        return new InstrumentedTcpHandler(nc);
    }

    private class InstrumentedTcpHandler extends TcpEventHandler<TestClusteredNetworkContext> {

        public InstrumentedTcpHandler(@NotNull TestClusteredNetworkContext nc) {
            super(nc);
        }

        @Override
        public void eventLoop(EventLoop eventLoop) {
            super.eventLoop(eventLoop);
            tcpHandlerAddedToEventLoop.set(true);
        }
    }
}