package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.threads.EventGroupBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ClusterAcceptorEventHandlerTest {

    @Test
    void willPopulateNetworkStatsListenerWhenNetworkStatsListenerFactorySpecified() throws IOException {
        TCPRegistry.createServerSocketChannelFor("testAcceptor");
        final MyClusterContext acceptorContext = new MyClusterContext();
        final NetworkStatsListener<MyClusteredNetworkContext> nsl = mock(NetworkStatsListener.class);
        acceptorContext.networkStatsListenerFactory(clusterContext -> nsl);
        final ClusterAcceptorEventHandler<MyClusterContext, MyClusteredNetworkContext> acceptorEventHandler
                = new ClusterAcceptorEventHandler<>("testAcceptor", acceptorContext);
        EventLoop eventLoop = EventGroupBuilder.builder().build();
        eventLoop.addHandler(acceptorEventHandler);
        eventLoop.start();
        final MyClusterContext initiaitorContext = new MyClusterContext();
        HostConnector<MyClusteredNetworkContext, MyClusterContext> connector
                = new HostConnector<>(initiaitorContext, new RemoteConnector<>(initiaitorContext.tcpEventHandlerFactory()), 1, "testAcceptor");
        connector.connect();
        initiaitorContext.eventLoop().start();
        while (acceptorContext.tcpEventHandlers.size() == 0) {
            Jvm.pause(10);
        }
        assertSame(nsl, acceptorContext.tcpEventHandlers.get(0).nc.networkStatsListener());
        verify(nsl).networkContext(any(MyClusteredNetworkContext.class));
    }

    static class MyClusterContext extends ClusterContext<MyClusterContext, MyClusteredNetworkContext> {

        private final List<NetworkContextExposingTcpEventHandler> tcpEventHandlers = new ArrayList<>();

        @Override
        public @NotNull ThrowingFunction<MyClusteredNetworkContext, TcpEventHandler<MyClusteredNetworkContext>, IOException> tcpEventHandlerFactory() {
            return this::createTcpEventHandler;
        }

        @NotNull
        private TcpEventHandler<MyClusteredNetworkContext> createTcpEventHandler(MyClusteredNetworkContext clusteredNetworkContext) {
            final NetworkContextExposingTcpEventHandler tcpEventHandler = new NetworkContextExposingTcpEventHandler(clusteredNetworkContext);
            tcpEventHandlers.add(tcpEventHandler);
            return tcpEventHandler;
        }

        @Override
        protected void defaults() {
            wireType(WireType.BINARY);
            wireOutPublisherFactory(VanillaWireOutPublisher::new);
            serverThreadingStrategy(ServerThreadingStrategy.SINGLE_THREADED);
            networkContextFactory(MyClusteredNetworkContext::new);
        }

        @Override
        protected String clusterNamePrefix() {
            return "testAcceptor";
        }
    }

    static class NetworkContextExposingTcpEventHandler extends TcpEventHandler<MyClusteredNetworkContext> {

        private final MyClusteredNetworkContext nc;

        public NetworkContextExposingTcpEventHandler(@NotNull MyClusteredNetworkContext nc) {
            super(nc);
            this.nc = nc;
        }
    }

    static class MyClusteredNetworkContext extends VanillaClusteredNetworkContext<MyClusteredNetworkContext, MyClusterContext> {

        public MyClusteredNetworkContext(@NotNull MyClusterContext clusterContext) {
            super(clusterContext);
        }
    }
}