/*
 * Copyright 2016-2020 Chronicle Software
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

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.network.tcp.ISocketChannel;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class HostConnector<T extends ClusteredNetworkContext<T>> extends AbstractCloseable {

    private final WireType wireType;

    private final Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private final List<WriteMarshallable> bootstraps = new LinkedList<>();

    private final RemoteConnector<T> remoteConnector;
    private final String connectUri;
    private final Function<ClusterContext<T>, T> networkContextFactory;
    @NotNull
    private final ClusterContext<T> clusterContext;
    private final Function<ClusterContext<T>, NetworkStatsListener<T>> networkStatsListenerFactory;
    private T nc;
    @NotNull
    private final AtomicReference<WireOutPublisher> wireOutPublisher = new AtomicReference<>();
    @NotNull
    private final EventLoop eventLoop;

    HostConnector(@NotNull ClusterContext<T> clusterContext,
                  final RemoteConnector<T> remoteConnector,
                  @NotNull final HostDetails hostdetails) {
        this.clusterContext = clusterContext;
        this.remoteConnector = remoteConnector;
        this.networkStatsListenerFactory = clusterContext.networkStatsListenerFactory();
        this.networkContextFactory = clusterContext.networkContextFactory();
        this.connectUri = hostdetails.connectUri();
        this.wireType = clusterContext.wireType();
        this.wireOutPublisherFactory = clusterContext.wireOutPublisherFactory();
        this.eventLoop = clusterContext.eventLoop();
    }

    @Override
    protected synchronized void performClose() {
        WireOutPublisher wp = wireOutPublisher.getAndSet(null);

        ISocketChannel socketChannel = nc.socketChannel();
        if (socketChannel != null) {
            Closeable.closeQuietly(socketChannel);
            Closeable.closeQuietly(socketChannel.socket());
        }

        if (wp != null)
            wp.close();

    }

    public synchronized void bootstrap(WriteMarshallable subscription) {
        bootstraps.add(subscription);
        WireOutPublisher wp = wireOutPublisher.get();
        if (wp != null) {
            wp.publish(subscription);
            wp.wireType(this.wireType);
        }
    }

    public synchronized void connect() {

        WireOutPublisher wireOutPublisher = wireOutPublisherFactory.apply(clusterContext.wireType());

        if (!this.wireOutPublisher.compareAndSet(null, wireOutPublisher)) {
            wireOutPublisher.close();
            return;
        }

        // we will send the initial header as text wire, then the rest will be sent in
        // what ever wire is configured
        nc = networkContextFactory.apply(clusterContext)
                .wireOutPublisher(wireOutPublisher)
                .isAcceptor(false)
                .heartbeatTimeoutMs(clusterContext.heartbeatTimeoutMs() * 2)
                .socketReconnector(this::reconnect)
                .serverThreadingStrategy(clusterContext.serverThreadingStrategy())
                .wireType(this.wireType);

        if (networkStatsListenerFactory != null) {
            final NetworkStatsListener<T> networkStatsListener = networkStatsListenerFactory.apply(clusterContext);
            nc.networkStatsListener(networkStatsListener);
            networkStatsListener.networkContext(nc);
        }

        for (WriteMarshallable bootstrap : bootstraps) {
            wireOutPublisher.publish(bootstrap);
            wireOutPublisher.wireType(this.wireType);
        }

        remoteConnector.connect(connectUri, eventLoop, nc, clusterContext.retryInterval());
    }

    synchronized void reconnect() {
        close();
        if (!nc.isAcceptor() && !isClosed())
            HostConnector.this.connect();
    }

    public String connectUri() {
        return connectUri;
    }
}