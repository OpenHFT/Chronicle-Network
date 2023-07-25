package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.cluster.handlers.HeartbeatHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.connection.ConnectorEventHandler;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class BetterHostConnector<T extends ClusteredNetworkContext<T>, C extends ClusterContext<C, T>> extends AbstractCloseable implements IHostConnector<T, C> {

    private final String name;
    private final C clusterContext;
    private final ConnectionStrategy connectionStrategy;
    private final int remoteId;
    private final String[] connectUris;
    private final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory;
    private ConnectorEventHandler<T> connectorEventHandler;


    public BetterHostConnector(@NotNull final C clusterContext,
                               final int remoteId,
                               final ConnectionStrategy connectionStrategy,
                               @NotNull final String[] connectUris,
                               @NotNull final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory) {
        this.clusterContext = clusterContext;
        this.remoteId = remoteId;
        this.connectUris = connectUris;
        this.tcpEventHandlerFactory = tcpEventHandlerFactory;
        this.name = clusterContext.localIdentifier() + " to " + remoteId;
        this.connectionStrategy = connectionStrategy != null ? connectionStrategy : new AlwaysStartOnPrimaryConnectionStrategy();
    }

    @Override
    public void connect() {
        if (connectUris == null || connectUris.length == 0 || Arrays.stream(connectUris).anyMatch(uri -> uri == null || uri.length() == 0)) {
            Jvm.warn().on(BetterHostConnector.class, "ConnectURI was null or empty, not attempting to connect. connection=" + name);
            return;
        }

        final SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(connectUris, this.name);
        connectorEventHandler = new ConnectorEventHandler<>(name, connectionStrategy, this::createNetworkContext, tcpEventHandlerFactory, () -> !isClosing(), socketAddressSupplier, clusterContext.eventLoop()::addHandler);
        clusterContext.eventLoop().addHandler(connectorEventHandler);
    }

    private T createNetworkContext() {
        @NotNull WireOutPublisher wireOutPublisher = clusterContext.wireOutPublisherFactory().apply(clusterContext.wireType());
        wireOutPublisher.connectionDescription(name);

        final T networkContext = clusterContext.networkContextFactory().apply(clusterContext)
                .wireOutPublisher(wireOutPublisher)
                .isAcceptor(false)
                .heartbeatTimeoutMs(clusterContext.heartbeatTimeoutMs() * 2)
                .serverThreadingStrategy(clusterContext.serverThreadingStrategy())
                .wireType(clusterContext.wireType());

        final Function<C, NetworkStatsListener<T>> networkStatsListenerFactory = clusterContext.networkStatsListenerFactory();
        if (networkStatsListenerFactory != null) {
            final NetworkStatsListener<T> networkStatsListener = networkStatsListenerFactory.apply(clusterContext);
            networkContext.networkStatsListener(networkStatsListener);
            networkStatsListener.networkContext(networkContext);
        }

        final SessionProvider sessionProvider = clusterContext.sessionProvider();
        if (sessionProvider != null) {
            wireOutPublisher.publish(wire -> {
                try (final DocumentContext ignored = wire.writingDocument(true)) {
                    sessionProvider.get().writeMarshallable(wire);
                }
            });
        }

        wireOutPublisher.publish(UberHandler.uberHandler(
                clusterContext.localIdentifier(),
                remoteId,
                clusterContext.wireType()));
        wireOutPublisher.publish(HeartbeatHandler.heartbeatHandler(
                clusterContext.heartbeatTimeoutMs(),
                clusterContext.heartbeatIntervalMs(),
                HeartbeatHandler.class.hashCode()));
        wireOutPublisher.wireType(clusterContext.wireType());

        return networkContext;
    }

    @Override
    protected void performClose() throws IllegalStateException {
        closeQuietly(connectorEventHandler);
    }
}
