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
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static net.openhft.chronicle.network.cluster.HeartbeatHandler.HEARTBEAT_EXECUTOR;

public class HostConnector implements Closeable {

    private final WireType wireType;

    //  private final WriteMarshallable header;
    private final Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private final List<WriteMarshallable> bootstraps = new LinkedList<>();

    private final RemoteConnector remoteConnector;
    private final String connectUri;

    private WireOutPublisher wireOutPublisher;
    private NetworkContext nc;

    private final Function<ClusterContext, NetworkContext> networkContextFactory;
    private final ClusterContext clusterContext;
    private volatile boolean isConnected;

    @NotNull
    private EventLoop eventLoop;

    HostConnector(@NotNull ClusterContext clusterContext,
                  final RemoteConnector remoteConnector,
                  final HostDetails hostdetails) {
        this.clusterContext = clusterContext;
        this.remoteConnector = remoteConnector;
        //final WriteMarshallable header = clusterContext.handlerFactory().apply(clusterContext,
        //        hostdetails);
        this.networkContextFactory = clusterContext.networkContextFactory();
        this.connectUri = hostdetails.connectUri();
        this.wireType = clusterContext.wireType();
        //  this.header = header;
        this.wireOutPublisherFactory = clusterContext.wireOutPublisherFactory();

        this.eventLoop = clusterContext.eventLoop();
        this.wireOutPublisher = wireOutPublisherFactory.apply(WireType.TEXT);
    }


    @Override
    public synchronized void close() {
        isConnected = false;
        Closeable.closeQuietly(wireOutPublisher);
        if (nc.socketChannel() != null)
            Closeable.closeQuietly(nc.socketChannel());
        wireOutPublisher.clear();
    }

    public synchronized void bootstrap(WriteMarshallable subscription) {
        bootstraps.add(subscription);

        if (isConnected && wireOutPublisher != null)
            wireOutPublisher.put("", subscription);

    }


    public synchronized void connect() {

        isConnected = true;

        // we will send the initial header as text wire, then the rest will be sent in
        // what ever wire is configured
        nc = networkContextFactory.apply(clusterContext);
        nc.wireOutPublisher(wireOutPublisher);
        nc.wireType(wireType);
        nc.closeTask(this);
        nc.heartbeatTimeoutMs(clusterContext.heartbeatTimeoutMs() * 2);
        nc.heartbeatListener(() -> {
            if (nc.socketChannel() != null)
                Closeable.closeQuietly(nc.socketChannel());
            wireOutPublisher.clear();
            reconnect();
        });

        wireOutPublisher.wireType(wireType);

        for (WriteMarshallable bootstrap : bootstraps) {
            wireOutPublisher.publish(bootstrap);
        }

        remoteConnector.connect(connectUri, eventLoop, nc, 1_000L);
    }

    public void reconnect() {
        // using HEARTBEAT_EXECUTOR to eliminate tail recursion
        HEARTBEAT_EXECUTOR.submit((Runnable) () -> {
            synchronized (HostConnector.this) {
                if (!nc.isAcceptor())
                    HostConnector.this.connect();
            }
        });
    }
}