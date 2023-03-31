package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.test.TestClusterContext;
import net.openhft.chronicle.testframework.Waiters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static net.openhft.chronicle.network.TCPRegistry.createServerSocketChannelFor;
import static net.openhft.chronicle.network.test.TestClusterContext.forHosts;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionListenerTest extends NetworkTestCommon {

    private HostDetails initiatorHost;
    private HostDetails acceptorHost;
    private CountingConnectionListener initiatorCounter;
    private CountingConnectionListener acceptorCounter;

    @BeforeEach
    void setUp() throws IOException {
        createServerSocketChannelFor("initiator", "acceptor");
        initiatorHost = new HostDetails().hostId(2).connectUri("initiator");
        acceptorHost = new HostDetails().hostId(1).connectUri("acceptor");
        acceptorCounter = new CountingConnectionListener();
        initiatorCounter = new CountingConnectionListener();
    }

    @Test
    void onConnectAndOnDisconnectAreCalledOnce_OnOrderlyConnectionAndDisconnection() {
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.addConnectionListener(acceptorCounter);
            initiatorCtx.addConnectionListener(initiatorCounter);

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            Waiters.waitForCondition("acceptor and initiator to connect",
                    () -> acceptorCounter.onConnectedCalls > 0 && initiatorCounter.onConnectedCalls > 0,
                    5_000);
        }
        assertEquals(1, acceptorCounter.onConnectedCalls);
        assertEquals(1, acceptorCounter.onDisconnectedCalls);
        assertEquals(1, initiatorCounter.onConnectedCalls);
        assertEquals(1, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void onConnectAndOnDisconnectAreCalledOnce_WhenConnectionTimesOut_InUberHandler() {
        ignoreException("THIS IS NOT AN ERROR");
        expectException("missed heartbeat, lastTimeMessageReceived=");
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {
            initiatorCtx.overrideNetworkContextTimeout(5_000); // we want the heartbeat handler to timeout
            initiatorCtx.disableReconnect();
            acceptorCtx.overrideNetworkContextTimeout(5_000); // we want the heartbeat handler to timeout
            acceptorCtx.disableReconnect();

            initiatorCtx.heartbeatTimeoutMs(1_000);  // set to minimum, initiator dictates

            acceptorCtx.addConnectionListener(acceptorCounter);
            initiatorCtx.addConnectionListener(initiatorCounter);

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            Waiters.waitForCondition("acceptor and initiator to connect",
                    () -> acceptorCounter.onConnectedCalls > 0 && initiatorCounter.onConnectedCalls > 0,
                    5_000);
            // jam up the acceptor event loop to trigger an initiator timeout
            acceptorCtx.cluster().clusterContext().eventLoop().addHandler(() -> {
                Jvm.pause(3_000);
                throw InvalidEventHandlerException.reusable();
            });
            Waiters.waitForCondition("initiator to timeout",
                    () -> initiatorCounter.onDisconnectedCalls > 0, 3_000);
        }
        assertEquals(1, acceptorCounter.onConnectedCalls);
        assertEquals(1, acceptorCounter.onDisconnectedCalls);
        assertEquals(1, initiatorCounter.onConnectedCalls);
        assertEquals(1, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void onConnectAndOnDisconnectAreCalledOnce_WhenConnectionTimesOut_InTcpHandler() {
        expectException("Missed heartbeat on network context");
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {
            initiatorCtx.overrideNetworkContextTimeout(1_000); // we want the TcpEventHandler to timeout
            initiatorCtx.disableReconnect();
            acceptorCtx.overrideNetworkContextTimeout(1_000); // we want the TcpEventHandler to timeout
            acceptorCtx.disableReconnect();

            initiatorCtx.heartbeatTimeoutMs(5_000);

            acceptorCtx.addConnectionListener(acceptorCounter);
            initiatorCtx.addConnectionListener(initiatorCounter);

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            Waiters.waitForCondition("acceptor and initiator to connect",
                    () -> acceptorCounter.onConnectedCalls > 0 && initiatorCounter.onConnectedCalls > 0,
                    5_000);

            // jam up the acceptor event loop to trigger an initiator timeout
            acceptorCtx.cluster().clusterContext().eventLoop().addHandler(() -> {
                Jvm.pause(3_000);
                throw InvalidEventHandlerException.reusable();
            });
            Waiters.waitForCondition("initiator to timeout",
                    () -> initiatorCounter.onDisconnectedCalls == 1, 3_000);
        }
        assertEquals(1, acceptorCounter.onConnectedCalls);
        assertEquals(1, acceptorCounter.onDisconnectedCalls);
        assertEquals(1, initiatorCounter.onConnectedCalls);
        assertEquals(1, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void onConnectAndOnDisconnectAreNotCalled_WhenNoConnectionIsEstablished_Initiator() {
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.addConnectionListener(acceptorCounter);
            initiatorCtx.addConnectionListener(initiatorCounter);

            // only start initiator
            initiatorCtx.cluster().start(initiatorHost.hostId());
            Jvm.pause(1_000);
        }
        assertEquals(0, acceptorCounter.onConnectedCalls);
        assertEquals(0, acceptorCounter.onDisconnectedCalls);
        assertEquals(0, initiatorCounter.onConnectedCalls);
        assertEquals(0, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void onConnectAndOnDisconnectAreNotCalled_WhenNoConnectionIsEstablished_Acceptor() {
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.addConnectionListener(acceptorCounter);
            initiatorCtx.addConnectionListener(initiatorCounter);

            // only start acceptor
            acceptorCtx.cluster().start(acceptorHost.hostId());
            Jvm.pause(1_000);
        }
        assertEquals(0, acceptorCounter.onConnectedCalls);
        assertEquals(0, acceptorCounter.onDisconnectedCalls);
        assertEquals(0, initiatorCounter.onConnectedCalls);
        assertEquals(0, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void onConnectAndOnDisconnect_WillLogWhenAnExceptionIsThrown() {
        expectException("Something went wrong - onConnect");
        expectException("Something went wrong - onDisconnect");
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.addConnectionListener(new ThrowingConnectionListener());
            initiatorCtx.addConnectionListener(initiatorCounter);

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            Waiters.waitForCondition("initiator to connect",
                    () -> initiatorCounter.onConnectedCalls > 0,
                    5_000);

            // this shouldn't trigger a disconnect
            Jvm.pause(1_000);
            assertEquals(0, initiatorCounter.onDisconnectedCalls);
        }
        assertEquals(1, initiatorCounter.onConnectedCalls);
        assertEquals(1, initiatorCounter.onDisconnectedCalls);
    }

    @Test
    void testNullConnectionListenerIsTolerated() {
        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.returnNullConnectionListener();
            initiatorCtx.addConnectionListener(initiatorCounter);

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            Waiters.waitForCondition("initiator to connect",
                    () -> initiatorCounter.onConnectedCalls > 0,
                    5_000);

            // this shouldn't trigger a disconnect
            Jvm.pause(1_000);
            assertEquals(0, initiatorCounter.onDisconnectedCalls);
        }
        assertEquals(1, initiatorCounter.onConnectedCalls);
        assertEquals(1, initiatorCounter.onDisconnectedCalls);
    }

    private static class ThrowingConnectionListener implements ConnectionListener {

        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            throw new RuntimeException("Something went wrong - onConnect");
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            throw new RuntimeException("Something went wrong - onDisconnect");
        }
    }

    private static class CountingConnectionListener implements ConnectionListener {

        private int onConnectedCalls = 0;
        private int onDisconnectedCalls = 0;

        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            onConnectedCalls++;
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            onDisconnectedCalls++;
        }
    }
}