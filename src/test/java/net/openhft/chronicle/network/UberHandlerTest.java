package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.cluster.VanillaClusteredNetworkContext;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

public class UberHandlerTest extends NetworkTestCommon {

    @Before
    public void before() {
        YamlLogging.setAll(true);
        System.setProperty("TcpEventHandler.tcpBufferSize", "131072");
    }

    @After
    public void after() {
        YamlLogging.setAll(false);
        System.clearProperty("TcpEventHandler.tcpBufferSize");
    }

    @Test
    public <T extends ClusteredNetworkContext<T>> void test() throws IOException {
        String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        HostDetails initiatorHost = new HostDetails().hostId(2).connectUri(desc);
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri(desc);

        MyClusterContext<T> acceptorCtx = clusterContext(acceptorHost);
        try (Cluster<T, MyClusterContext<T>> ignored = acceptorCtx.cluster()) {
            acceptorCtx.accept(acceptorHost);

            MyClusterContext<T> initiatorCtx = clusterContext(initiatorHost);
            try (Cluster<T, MyClusterContext<T>> ignored2 = initiatorCtx.cluster()) {
                initiatorCtx.connect(acceptorHost);

                initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                    System.out.println("connection changed, isConnected " + isConnected);
                });

                acceptorCtx.eventLoop().start();
                initiatorCtx.eventLoop().start();

                // TODO: set HB numbers relatively low
                // TODO: add handlers to deterministically pause
                // TODO: send in a large message (>128K) to make bytes resize happen

                Jvm.pause(1_000);
            }
        }
    }

    @NotNull
    private <T extends ClusteredNetworkContext<T>> MyClusterContext<T> clusterContext(HostDetails hd) {
        Cluster<T, MyClusterContext<T>> cluster = new MyCluster();
        MyClusterContext<T> ctx = new MyClusterContext<T>().wireType(WireType.TEXT).localIdentifier((byte) hd.hostId());
        cluster.clusterContext(ctx);
        return ctx;
    }

    static class MyCluster<T extends ClusteredNetworkContext<T>> extends Cluster<T, MyClusterContext<T>> {
        MyCluster() {
            super();
        }
    }

    static class MyClusterContext<T extends ClusteredNetworkContext<T>> extends net.openhft.chronicle.network.cluster.ClusterContext<MyClusterContext<T>, T> {
        @Override
        protected String clusterNamePrefix() {
            return "";
        }

        @NotNull
        @Override
        public ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory() {
            return nc -> {
                final TcpEventHandler<T> handler = new TcpEventHandler<>(nc);
                final Function<T, TcpHandler<T>> factory = unused -> new HeaderTcpHandler<>(handler, o -> (TcpHandler<T>) o);
                final WireTypeSniffingTcpHandler<T> sniffer = new WireTypeSniffingTcpHandler<>(handler, factory);
                handler.tcpHandler(sniffer);
                return handler;
            };
        }

        @Override
        protected void defaults() {
            if (this.wireOutPublisherFactory() == null)
                this.wireOutPublisherFactory(VanillaWireOutPublisher::new);

//            if (wireType() == null)
//                this.wireType(WireType.BINARY_LIGHT);

            if (serverThreadingStrategy() == null)
                this.serverThreadingStrategy(ServerThreadingStrategy.SINGLE_THREADED);

            if (this.networkContextFactory() == null)
                this.networkContextFactory(cc -> (T) new VanillaClusteredNetworkContext(cc));

            //////////////////////////////////////
//            if (this.networkStatsListenerFactory() == null)
//                this.networkStatsListenerFactory(ctx -> LoggingNetworkStatsListener.INSTANCE);
        }
    }
}
