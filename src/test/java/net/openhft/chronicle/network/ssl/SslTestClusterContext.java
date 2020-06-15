package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.cluster.handlers.HeartbeatHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public final class SslTestClusterContext extends ClusterContext<SslTestClusteredNetworkContext> {

    private transient SslTestCluster cluster;

    private static <T extends SslNetworkContext<T>> TcpHandler<T> wrapForSsl(final TcpHandler<T> delegate) {
        new RuntimeException(String.format("%s/0x%s created",
                delegate.getClass().getSimpleName(), Integer.toHexString(System.identityHashCode(delegate)))).
                printStackTrace(System.out);

        return new SslDelegatingTcpHandler<>(delegate);
    }

    @Override
    public void defaults() {
        this.handlerFactory(new UberHandler.Factory<>())
                .heartbeatFactory(new HeartbeatHandler.Factory<>())
                .wireOutPublisherFactory(VanillaWireOutPublisher::new)
                .wireType(WireType.TEXT)
                .connectionEventHandler(StubConnectionManager::new)
                .serverThreadingStrategy(ServerThreadingStrategy.CONCURRENT)
                .networkContextFactory(c -> ncFactory())
                .networkStatsListenerFactory(ctx -> new LoggingNetworkStatsListener<>())
                .eventLoop(new EventGroup(false, Pauser.balanced(), false, "ssl-cluster-"));
    }

    @NotNull
    private SslTestClusteredNetworkContext ncFactory() {
        return new SslTestClusteredNetworkContext(localIdentifier(), this.cluster, eventLoop());
    }

    @NotNull
    @Override
    public ThrowingFunction<SslTestClusteredNetworkContext, TcpEventHandler<SslTestClusteredNetworkContext>, IOException> tcpEventHandlerFactory() {
        return new BootstrapHandlerFactory<SslTestClusteredNetworkContext>()::createHandler;
    }

    void cluster(final SslTestCluster cluster) {

        this.cluster = cluster;
    }

    public static final class BootstrapHandlerFactory<T extends NetworkContext<T>> {
        @NotNull
        TcpEventHandler<T> createHandler(final T networkContext) {
            @NotNull final T nc = networkContext;
            if (nc.isAcceptor())
                nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
            @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);

            @NotNull final Function<Object, TcpHandler<T>> consumer = o -> {

                if (o instanceof TcpHandler) {
                    return wrapForSsl((TcpHandler) o);
                }

                throw new UnsupportedOperationException("not supported class=" + o.getClass());
            };

            final NetworkStatsListener<T> nl = nc.networkStatsListener();
            notifyHostPort(nc.socketChannel(), nl);

            @Nullable final Function<T, TcpHandler<T>> f
                    = x -> new HeaderTcpHandler<>(handler, consumer);

            @NotNull final WireTypeSniffingTcpHandler<T> sniffer = new
                    WireTypeSniffingTcpHandler<>(handler, f);

            handler.tcpHandler(sniffer);
            return handler;
        }
    }

    private static class StubConnectionManager<T extends NetworkContext<T>> implements ConnectionManager<T> {
        private final List<ConnectionListener<T>> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onConnectionChanged(final boolean isConnected, final T nc) {
            for (ConnectionListener<T> listener : listeners) {
                listener.onConnectionChange(nc, isConnected);
            }
        }

        @Override
        public void addListener(final ConnectionListener<T> connectionListener) {
            listeners.add(connectionListener);
        }
    }

    private static class LoggingNetworkStatsListener<T extends NetworkContext<T>> implements NetworkStatsListener<T> {
        @Override
        public void networkContext(final T networkContext) {

        }

        @Override
        public void onNetworkStats(final long writeBps, final long readBps, final long socketPollCountPerSecond) {

        }

        @Override
        public void onHostPort(final String hostName, final int port) {
        }

        @Override
        public void onRoundTripLatency(final long nanosecondLatency) {

        }

        @Override
        public void procPrefix(final String procPrefix) {

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
