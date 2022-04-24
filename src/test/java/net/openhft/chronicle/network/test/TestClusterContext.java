package net.openhft.chronicle.network.test;

import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.apache.mina.util.IdentityHashSet;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

/**
 * A very minimal {@link ClusterContext} implementation for use in tests
 */
public class TestClusterContext extends ClusterContext<TestClusterContext, TestClusteredNetworkContext> {

    private final Set<ConnectionListener> connectionListeners = new IdentityHashSet<>();
    private Long overrideNetworkContextTimeout;
    private boolean disableReconnect;
    private boolean returnNullConnectionListener;

    @NotNull
    public static TestClusterContext forHosts(HostDetails... clusterHosts) {
        TestClusterContext ctx = new TestClusterContext().wireType(WireType.BINARY).localIdentifier((byte) clusterHosts[0].hostId());
        ctx.heartbeatIntervalMs(500);
        TestCluster cluster = new TestCluster(ctx);
        for (HostDetails details : clusterHosts) {
            cluster.hostDetails.put(String.valueOf(details.hostId()), details);
        }
        return ctx;
    }

    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    public void disableReconnect() {
        disableReconnect = true;
    }

    public void returnNullConnectionListener() {
        this.returnNullConnectionListener = true;
    }

    public TestClusterContext overrideNetworkContextTimeout(long overrideNetworkContextTimeout) {
        this.overrideNetworkContextTimeout = overrideNetworkContextTimeout;
        return this;
    }

    @Override
    protected String clusterNamePrefix() {
        return "";
    }

    @NotNull
    @Override
    public ThrowingFunction<TestClusteredNetworkContext, TcpEventHandler<TestClusteredNetworkContext>, IOException> tcpEventHandlerFactory() {
        return nc -> {
            if (nc.isAcceptor()) {
                nc.wireOutPublisher(new VanillaWireOutPublisher(wireType()));
            }
            final TcpEventHandler<TestClusteredNetworkContext> handler = new TcpEventHandler<>(nc);
            final Function<TestClusteredNetworkContext, TcpHandler<TestClusteredNetworkContext>> factory =
                    unused -> new HeaderTcpHandler<>(handler, o -> (TcpHandler<TestClusteredNetworkContext>) o);
            final WireTypeSniffingTcpHandler<TestClusteredNetworkContext> sniffer = new WireTypeSniffingTcpHandler<>(handler, factory);
            handler.tcpHandler(sniffer);
            return handler;
        };
    }

    @Override
    protected void defaults() {
        if (this.wireType() == null)
            this.wireType(WireType.BINARY);

        if (this.wireOutPublisherFactory() == null)
            this.wireOutPublisherFactory(VanillaWireOutPublisher::new);

        if (serverThreadingStrategy() == null)
            this.serverThreadingStrategy(ServerThreadingStrategy.SINGLE_THREADED);

        if (this.networkContextFactory() == null)
            this.networkContextFactory((TestClusterContext clusterContext) -> {
                final TestClusteredNetworkContext nc = new TestClusteredNetworkContext(clusterContext);
                connectionListeners.forEach(nc::addConnectionListener);
                if (overrideNetworkContextTimeout != null) {
                    nc.heartbeatTimeoutMsOverride(overrideNetworkContextTimeout);
                }
                if (disableReconnect) {
                    nc.disableReconnect();
                }
                if (returnNullConnectionListener) {
                    nc.returnNullConnectionListener();
                }
                return nc;
            });
    }
}
