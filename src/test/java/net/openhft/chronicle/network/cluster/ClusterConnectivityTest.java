package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ThrowingRunnable;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.test.TestCluster;
import net.openhft.chronicle.network.test.TestClusterContext;
import net.openhft.chronicle.testframework.internal.network.proxy.TcpProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.util.TestUtil.getAvailablePortNumber;
import static net.openhft.chronicle.testframework.ExecutorServiceUtil.shutdownAndWaitForTermination;
import static net.openhft.chronicle.testframework.Waiters.waitForCondition;

class ClusterConnectivityTest extends NetworkTestCommon {

    private static final Consumer<TestClusterContext> NO_OP = cc -> {
    };
    private ExecutorService executorService;
    private TcpProxy hostOneProxy;
    private TcpProxy hostTwoProxy;
    private boolean[][] connectedMatrix;

    @BeforeEach
    void setUp() throws IOException {
        connectedMatrix = new boolean[3][3];
        executorService = Executors.newCachedThreadPool();
        TCPRegistry.createServerSocketChannelFor("host.one", "host.two", "host.three");
        hostOneProxy = new TcpProxy(getAvailablePortNumber(), TCPRegistry.lookup("host.one"), executorService);
        hostTwoProxy = new TcpProxy(getAvailablePortNumber(), TCPRegistry.lookup("host.two"), executorService);
        executorService.submit(hostOneProxy);
        executorService.submit(hostTwoProxy);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(hostOneProxy, hostTwoProxy);
        shutdownAndWaitForTermination(executorService);
    }

    @Test
    void testNodesWillConnectToEachOther() {
        startClusterAnd(() -> waitForCondition("All nodes didn't connect", this::everyoneIsConnected, 5_000));
    }

    @Test
    void testNodesWillReconnectAfterDisconnection() {
        startClusterAnd(() -> {
            waitForCondition("All nodes didn't connect", this::everyoneIsConnected, 5_000);

            // Simulate network event that breaks connectivity to 1
            hostOneProxy.dropConnectionsAndPauseNewConnections();
            waitForCondition("Some nodes are still connected to 1", () -> this.nobodyIsConnectedTo(1), 5_000);

            // Allow connections again
            hostOneProxy.acceptNewConnections();
            waitForCondition("Connection wasn't restored", this::everyoneIsConnected, 5_000);
        });
    }

    @Test
    void testNodesWillReconnectAfterDisconnectionDueToTimeout() {
        expectException("Missed heartbeat on network context");
        startClusterAnd(() -> {
            waitForCondition("All nodes didn't connect", this::everyoneIsConnected, 5_000);

            // Simulate network event that interrupts traffic to 1
            hostOneProxy.stopForwardingTrafficAndPauseNewConnections();
            waitForCondition("Some nodes are still connected to 1", () -> this.nobodyIsConnectedTo(1), 5_000);

            // Allow connections again
            hostOneProxy.acceptNewConnections();
            waitForCondition("Connection wasn't restored", this::everyoneIsConnected, 5_000);
        }, cc -> cc.overrideNetworkContextTimeout(500));
    }

    @Test
    void testNodesWillReconnectAfterDisconnectionDueToApplicationLayerTimeout() {
        expectException("missed heartbeat, lastTimeMessageReceived");
        ignoreException("Missed heartbeat on network context");
        startClusterAnd(() -> {
            waitForCondition("All nodes didn't connect", this::everyoneIsConnected, 5_000);

            // Simulate network event that interrupts traffic to 1
            hostOneProxy.stopForwardingTrafficAndPauseNewConnections();
            waitForCondition("Some nodes are still connected to 1", () -> this.nobodyIsConnectedTo(1), 5_000);

            // Allow connections again
            hostOneProxy.acceptNewConnections();
            waitForCondition("Connection wasn't restored: " + connectionStatuses().map(Object::toString).collect(Collectors.joining(", ")), this::everyoneIsConnected, 5_000);
        }, cc -> cc.heartbeatTimeoutMs(1_000).heartbeatIntervalMs(500));
    }

    private <T extends Exception> void startClusterAnd(ThrowingRunnable<T> runnable) throws T {
        startClusterAnd(runnable, NO_OP);
    }

    private <T extends Exception> void startClusterAnd(ThrowingRunnable<T> runnable, Consumer<TestClusterContext> clusterContextConfigurer) throws T {
        try (TestCluster host1cluster = createCluster(1, clusterContextConfigurer);
             TestCluster host2cluster = createCluster(2, clusterContextConfigurer);
             TestCluster host3cluster = createCluster(3, clusterContextConfigurer)) {
            ConnectionListener matrixUpdater = new MatrixUpdatingConnectionListener();
            host1cluster.clusterContext().addConnectionListener(matrixUpdater);
            host2cluster.clusterContext().addConnectionListener(matrixUpdater);
            host3cluster.clusterContext().addConnectionListener(matrixUpdater);
            host1cluster.start(1);
            host2cluster.start(2);
            host3cluster.start(3);

            runnable.run();
        }
    }

    private TestCluster createCluster(int forHost, Consumer<TestClusterContext> clusterContextConfigurer) {
        final TestClusterContext clusterContext = new TestClusterContext();
        clusterContextConfigurer.accept(clusterContext);
        TestCluster cluster = new TestCluster(clusterContext);
        cluster.hostDetails.put("host1", new HostDetails().hostId(1).connectUri("host.one"));
        cluster.hostDetails.put("host2", new HostDetails().hostId(2).connectUri("host.two"));
        cluster.hostDetails.put("host3", new HostDetails().hostId(3).connectUri("host.three"));
        switch (forHost) {
            case 3:
                cluster.hostDetails.get("host2").connectUri("localhost:" + hostTwoProxy.socketAddress().getPort());
                // fall through
            case 2:
                cluster.hostDetails.get("host1").connectUri("localhost:" + hostOneProxy.socketAddress().getPort());
            default:
        }
        return cluster;
    }

    private class MatrixUpdatingConnectionListener implements ConnectionListener {

        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.startup().on(MatrixUpdatingConnectionListener.class, localIdentifier + " connected to " + remoteIdentifier);
            connectedMatrix[localIdentifier - 1][remoteIdentifier - 1] = true;
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.startup().on(MatrixUpdatingConnectionListener.class, localIdentifier + " disconnected from " + remoteIdentifier);
            connectedMatrix[localIdentifier - 1][remoteIdentifier - 1] = false;
        }
    }

    private boolean nobodyIsConnectedTo(int hostId) {
        return connectionStatuses()
                .filter(cs -> cs.localHostId == hostId || cs.remoteHostId == hostId)
                .noneMatch(cs -> cs.connected);
    }

    private boolean everyoneIsConnected() {
        return connectionStatuses().allMatch(cs -> cs.connected);
    }

    private Stream<ConnectionStatus> connectionStatuses() {
        return IntStream.range(0, connectedMatrix.length)
                .boxed()
                .flatMap(i -> IntStream.range(0, connectedMatrix.length)
                        .filter(j -> j != i)
                        .mapToObj(j -> new ConnectionStatus(i + 1, j + 1, connectedMatrix[i][j])));
    }

    private static class ConnectionStatus {
        final int localHostId;
        final int remoteHostId;
        final boolean connected;

        private ConnectionStatus(int localHostId, int remoteHostId, boolean connected) {
            this.localHostId = localHostId;
            this.remoteHostId = remoteHostId;
            this.connected = connected;
        }

        @Override
        public String toString() {
            return "ConnectionStatus{" +
                    "localHostId=" + localHostId +
                    ", remoteHostId=" + remoteHostId +
                    ", connected=" + connected +
                    '}';
        }
    }
}
