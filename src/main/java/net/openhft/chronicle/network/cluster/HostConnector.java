/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class HostConnector implements Closeable {

    private final WireType wireType;

    //  private final WriteMarshallable header;
    private final Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private final List<WriteMarshallable> bootstraps = new LinkedList<>();

    private final RemoteConnector remoteConnector;
    private final String connectUri;
    private final Function<ClusterContext, NetworkContext> networkContextFactory;
    @NotNull
    private final ClusterContext clusterContext;
    private final Function<ClusterContext, NetworkStatsListener> networkStatsListenerFactory;
    private NetworkContext nc;
    @NotNull
    private volatile AtomicReference<WireOutPublisher> wireOutPublisher = new AtomicReference<>();
    @NotNull
    private EventLoop eventLoop;

    HostConnector(@NotNull ClusterContext clusterContext,
                  final RemoteConnector remoteConnector,
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
    public synchronized void close() {
        WireOutPublisher wp = wireOutPublisher.getAndSet(null);

        SocketChannel socketChannel = nc.socketChannel();
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
        nc = networkContextFactory.apply(clusterContext);
        nc.wireOutPublisher(wireOutPublisher);
        nc.isAcceptor(false);

        nc.heartbeatTimeoutMs(clusterContext.heartbeatTimeoutMs() * 2);
        nc.socketReconnector(this::reconnect);
        nc.serverThreadingStrategy(clusterContext.serverThreadingStrategy());

        if (networkStatsListenerFactory != null) {
            final NetworkStatsListener networkStatsListener = networkStatsListenerFactory.apply(clusterContext);
            nc.networkStatsListener(networkStatsListener);
            networkStatsListener.networkContext(nc);
        }

        for (WriteMarshallable bootstrap : bootstraps) {
            wireOutPublisher.publish(bootstrap);
            wireOutPublisher.wireType(this.wireType);
        }

        nc.wireType(this.wireType);

        remoteConnector.connect(connectUri, eventLoop, nc, clusterContext.retryInterval());
    }

    synchronized void reconnect() {
        close();
        if (!nc.isAcceptor())
            HostConnector.this.connect();

    }
}