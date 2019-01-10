package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.io.IORuntimeException;
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
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public final class SslTestClusterContext extends ClusterContext {

    private SslTestCluster cluster;

    SslTestClusterContext(@NotNull final WireIn wire) throws IORuntimeException {
        super(wire);
    }

    private static TcpHandler wrapForSsl(final TcpHandler delegate) {
        new RuntimeException(String.format("%s/0x%s created",
                delegate.getClass().getSimpleName(), Integer.toHexString(System.identityHashCode(delegate)))).
                printStackTrace(System.out);

//        return delegate;

        return new SslDelegatingTcpHandler(delegate);
    }

    @Override
    public void defaults() {
        this.handlerFactory(new UberHandler.Factory());
        this.heartbeatFactory(new HeartbeatHandler.Factory());
        this.wireOutPublisherFactory(VanillaWireOutPublisher::new);
        this.wireType(WireType.TEXT);
        this.connectionEventHandler(StubConnectionManager::new);
        this.serverThreadingStrategy(ServerThreadingStrategy.CONCURRENT);
        this.networkContextFactory(c -> ncFactory());
        this.networkStatsListenerFactory(ctx -> new LoggingNetworkStatsListener());
        this.eventLoop(new EventGroup(false, Pauser.balanced(), false, "ssl-cluster-"));
    }

    @NotNull
    private SslTestClusteredNetworkContext ncFactory() {
        return new SslTestClusteredNetworkContext(localIdentifier(), this.cluster, eventLoop());
    }

    @NotNull
    @Override
    public ThrowingFunction<NetworkContext, TcpEventHandler, IOException> tcpEventHandlerFactory() {
        return new BootstrapHandlerFactory()::createHandler;
    }

    void cluster(final SslTestCluster cluster) {

        this.cluster = cluster;
    }

    public static final class BootstrapHandlerFactory {
        @NotNull
        TcpEventHandler createHandler(final NetworkContext networkContext) {
            @NotNull final NetworkContext nc = networkContext;
            if (nc.isAcceptor())
                nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
            @NotNull final TcpEventHandler handler = new TcpEventHandler(networkContext);

            @NotNull final Function<Object, TcpHandler> consumer = o -> {

                if (o instanceof TcpHandler) {
                    return wrapForSsl((TcpHandler) o);
                }

                throw new UnsupportedOperationException("not supported class=" + o.getClass());
            };

            final NetworkStatsListener nl = nc.networkStatsListener();
            if (nl != null)
                notifyHostPort(nc.socketChannel(), nl);

            @Nullable final Function<NetworkContext, TcpHandler> f
                    = x -> new HeaderTcpHandler<>(handler, consumer, x);

            @NotNull final WireTypeSniffingTcpHandler sniffer = new
                    WireTypeSniffingTcpHandler<>(handler, f);

            handler.tcpHandler(sniffer);
            return handler;
        }

    }

    private static class StubConnectionManager implements ConnectionManager {
        private final List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onConnectionChanged(final boolean isConnected, final NetworkContext nc) {
            for (ConnectionListener listener : listeners) {
                listener.onConnectionChange(nc, isConnected);
            }
        }

        @Override
        public void addListener(final ConnectionListener connectionListener) {
            listeners.add(connectionListener);
        }
    }

    private static class LoggingNetworkStatsListener implements NetworkStatsListener {
        @Override
        public void networkContext(final NetworkContext networkContext) {

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
    }
}
