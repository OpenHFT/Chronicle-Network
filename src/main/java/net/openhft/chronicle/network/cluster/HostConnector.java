/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.cluster.handlers.HeartbeatHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class HostConnector<T extends ClusteredNetworkContext<T>, C extends ClusterContext<C, T>> implements Closeable {

    private interface ClosableRunnable extends Runnable, Closeable {
    }

    @NotNull
    private final ConnectionManager<T> connectionManager;

    private final WireType wireType;

    private final Function<WireType, WireOutPublisher> wireOutPublisherFactory;

    private final RemoteConnector<T> remoteConnector;
    private final String connectUri;
    private final Function<C, T> networkContextFactory;
    @NotNull
    private final C clusterContext;
    private final Function<C, NetworkStatsListener<T>> networkStatsListenerFactory;
    private final int remoteId;
    private volatile T nc;
    @NotNull
    private final AtomicReference<WireOutPublisher> wireOutPublisher = new AtomicReference<>();
    @NotNull
    private final EventLoop eventLoop;

    HostConnector(@NotNull final C clusterContext,
                  @NotNull final RemoteConnector<T> remoteConnector,
                  final int remoteId,
                  final String connectUri) {
        this.connectionManager = clusterContext.connectionManager(remoteId);
        this.clusterContext = clusterContext;
        this.remoteId = remoteId;
        this.remoteConnector = remoteConnector;
        this.networkStatsListenerFactory = clusterContext.networkStatsListenerFactory();
        this.networkContextFactory = clusterContext.networkContextFactory();
        this.connectUri = connectUri;
        this.wireType = clusterContext.wireType();
        this.wireOutPublisherFactory = clusterContext.wireOutPublisherFactory();
        this.eventLoop = clusterContext.eventLoop();
    }

    @Override
    public synchronized void close() {

        WireOutPublisher wp = wireOutPublisher.getAndSet(null);
        closeQuietly(wp);

        final T nc = this.nc;
        if (nc == null)
            return;

        ChronicleSocketChannel socketChannel = nc.socketChannel();
        if (socketChannel != null) {
            closeQuietly(socketChannel, socketChannel.socket());
        }

        closeQuietly(nc);
        this.nc = null;
    }

    public ConnectionManager<T> connectionManager() {
        return connectionManager;
    }

    public synchronized void connect() {

        if (connectUri == null || connectUri.isEmpty())
            return;

        WireOutPublisher wireOutPublisher = wireOutPublisherFactory.apply(clusterContext.wireType());
        wireOutPublisher.connectionDescription(clusterContext.localIdentifier() + " to " + remoteId);

        if (!this.wireOutPublisher.compareAndSet(null, wireOutPublisher)) {
            wireOutPublisher.close();
            return;
        }

        if (eventLoop.isClosed())
            return;

        closeQuietly(nc);

        // we will send the initial header as text wire, then the rest will be sent in
        // what ever wire is configured
        nc = networkContextFactory.apply(clusterContext)
                .wireOutPublisher(wireOutPublisher)
                .isAcceptor(false)
                .heartbeatTimeoutMs(clusterContext.heartbeatTimeoutMs() * 2)
                .socketReconnector(new ClosableRunnable() {
                    @Override
                    public void run() {
                        close();
                        if (!eventLoop.isClosing())
                            connect();
                    }

                    @Override
                    public void close() {
                        HostConnector.this.close();
                    }
                })
                .serverThreadingStrategy(clusterContext.serverThreadingStrategy())
                .wireType(this.wireType);

        if (networkStatsListenerFactory != null) {
            final NetworkStatsListener<T> networkStatsListener = networkStatsListenerFactory.apply(clusterContext);
            nc.networkStatsListener(networkStatsListener);
            networkStatsListener.networkContext(nc);
        }

        wireOutPublisher.publish(UberHandler.uberHandler(
                clusterContext.localIdentifier(),
                remoteId,
                wireType));
        wireOutPublisher.publish(HeartbeatHandler.heartbeatHandler(
                clusterContext.heartbeatTimeoutMs(),
                clusterContext.heartbeatIntervalMs(),
                HeartbeatHandler.class.hashCode()));
        wireOutPublisher.wireType(this.wireType);

        remoteConnector.connect(connectUri, eventLoop, nc, clusterContext.retryInterval());
    }
}