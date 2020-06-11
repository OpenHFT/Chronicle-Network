package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Ignore
public final class SslClusterIntegrationTest extends NetworkTestCommon {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(
                SslTestClusterContext.class
        );
    }

    private final SslTestCluster nodeOne = new SslTestCluster("cluster");
    private final SslTestCluster nodeTwo = new SslTestCluster("cluster");

    @Before
    public void setUp() throws Exception {
        TextWire.fromFile("src/test/resources/ssl-test-cluster.yaml").read().marshallable(nodeOne);
        TextWire.fromFile("src/test/resources/ssl-test-cluster.yaml").read().marshallable(nodeTwo);
        TCPRegistry.createServerSocketChannelFor("host1.port", "host2.port");
    }

    @Test
    public void shouldManageBidirectionalCommunication() throws Exception {
        startHost(1, nodeOne);
        startHost(2, nodeOne);

        LockSupport.parkNanos(TimeUnit.DAYS.toNanos(1));
    }

    private void startHost(final int hostId, final SslTestCluster node) throws IOException {
        final SslTestClusterContext clusterContext = Objects.requireNonNull(node.clusterContext());
        clusterContext.cluster(node);
        clusterContext.localIdentifier((byte) hostId);
        node.install();
        clusterContext.eventLoop().start();
        final String connectUri = node.findHostDetails(hostId).connectUri();
        final AcceptorEventHandler<SslTestClusteredNetworkContext> acceptorEventHandler = new AcceptorEventHandler<>(connectUri,
                new SslTestClusterContext.BootstrapHandlerFactory<SslTestClusteredNetworkContext>()::createHandler,
                () -> new SslTestClusteredNetworkContext((byte) hostId, node, clusterContext.eventLoop()));

        clusterContext.eventLoop().addHandler(acceptorEventHandler);

    }
}