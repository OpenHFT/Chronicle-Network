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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.ServerThreadingStrategy;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler.Factory;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class ClusterContext<T extends ClusteredNetworkContext<T>>
        extends SelfDescribingMarshallable
        implements Consumer<HostDetails>, Closeable {

    private transient Factory<T> handlerFactory;
    private transient Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private transient Function<ClusterContext<T>, T> networkContextFactory;
    private transient Function<ClusterContext<T>, WriteMarshallable> heartbeatFactory;
    private transient Function<ClusterContext<T>, NetworkStatsListener<T>> networkStatsListenerFactory;
    private transient Supplier<ConnectionManager<T>> connectionEventHandler;
    private transient EventLoop eventLoop;
    private transient boolean closed = false;
    private long heartbeatTimeoutMs = 40_000;
    private long heartbeatIntervalMs = 20_000;
    private ConnectionNotifier connectionNotifier;
    private WireType wireType;
    private String clusterName;
    private byte localIdentifier;
    private ServerThreadingStrategy serverThreadingStrategy;
    private long retryInterval = 1_000L;
    private String procPrefix;

    public ClusterContext() {
        defaults();
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        performClose();
    }

    protected void performClose() {
        Closeable.closeQuietly(
                connectionNotifier,
                handlerFactory,
                wireOutPublisherFactory,
                networkContextFactory,
                heartbeatFactory,
                networkStatsListenerFactory,
                connectionEventHandler,
                eventLoop);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public String procPrefix() {
        return procPrefix;
    }

    public void procPrefix(String procPrefix) {
        this.procPrefix = procPrefix;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wire.read("networkStatsListenerFactory").object(networkStatsListenerFactory, Function.class);
        defaults();
        super.readMarshallable(wire);
    }

    public Function<ClusterContext<T>, NetworkStatsListener<T>> networkStatsListenerFactory() {
        return networkStatsListenerFactory;
    }

    @NotNull
    public ClusterContext<T> networkStatsListenerFactory(Function<ClusterContext<T>, NetworkStatsListener<T>> networkStatsListenerFactory) {
        this.networkStatsListenerFactory = networkStatsListenerFactory;
        return this;
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    @NotNull
    public abstract ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory();

    public ClusterContext<T> serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        this.serverThreadingStrategy = serverThreadingStrategy;
        return this;
    }

    public ServerThreadingStrategy serverThreadingStrategy() {
        return serverThreadingStrategy;
    }

    private UberHandler.Factory<T> handlerFactory() {
        return handlerFactory;
    }

    public ClusterContext<T> handlerFactory(UberHandler.Factory<T> handlerFactory) {
        this.handlerFactory = handlerFactory;
        return this;
    }

    public void clusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }

    @NotNull
    public ClusterContext<T> eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    protected abstract void defaults();

    @NotNull
    public ClusterContext<T> localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
        return this;
    }

    @NotNull
    public ClusterContext<T> wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    @NotNull
    public ClusterContext<T> heartbeatFactory(Function<ClusterContext<T>, WriteMarshallable>
                                                      heartbeatFactor) {
        this.heartbeatFactory = heartbeatFactor;
        return this;
    }

    @NotNull
    public ClusterContext<T> heartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return this;
    }

    @NotNull
    public ClusterContext<T> heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return this;
    }

    @NotNull
    public ClusterContext<T> wireOutPublisherFactory(Function<WireType, WireOutPublisher> wireOutPublisherFactory) {
        this.wireOutPublisherFactory = wireOutPublisherFactory;
        return this;
    }

    @NotNull
    public ClusterContext<T> networkContextFactory(Function<ClusterContext<T>, T> networkContextFactory) {
        this.networkContextFactory = networkContextFactory;
        return this;
    }

    public WireType wireType() {
        return wireType;
    }

    public Function<WireType, WireOutPublisher> wireOutPublisherFactory() {
        return wireOutPublisherFactory;
    }

    public long heartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public String clusterName() {
        return clusterName;
    }

    public byte localIdentifier() {
        return localIdentifier;
    }

    public Function<ClusterContext<T>, T> networkContextFactory() {
        return networkContextFactory;
    }

    @NotNull
    public ClusterContext<T> connectionNotifier(ConnectionNotifier connectionNotifier) {
        this.connectionNotifier = connectionNotifier;
        return this;
    }

    private ConnectionNotifier connectionNotifier() {
        return this.connectionNotifier;
    }

    private Supplier<ConnectionManager<T>> connectionEventHandler() {
        return connectionEventHandler;
    }

    @NotNull
    public ClusterContext<T> connectionEventHandler(Supplier<ConnectionManager<T>> connectionEventHandler) {
        this.connectionEventHandler = connectionEventHandler;
        return this;
    }

    private Function<ClusterContext<T>, WriteMarshallable> heartbeatFactory() {
        return heartbeatFactory;
    }

    @Override
    public void accept(@NotNull HostDetails hd) {
        if (this.localIdentifier == hd.hostId())
            return;

        final ConnectionNotifier connectionNotifier = this.connectionNotifier();
        hd.connectionNotifier(connectionNotifier);

        final ConnectionManager<T> connectionManager = this
                .connectionEventHandler().get();
        hd.connectionManager(connectionManager);

        @NotNull final HostConnector<T> hostConnector = new HostConnector<>(this,
                new RemoteConnector<>(this.tcpEventHandlerFactory()),
                hd);

        hd.hostConnector(hostConnector);

        @NotNull ClusterNotifier clusterNotifier = new ClusterNotifier(connectionManager,
                hostConnector, bootstraps(hd));

        hd.clusterNotifier(clusterNotifier);
        hd.terminationEventHandler(clusterNotifier);

        clusterNotifier.connect();
    }

    @NotNull
    private List<WriteMarshallable> bootstraps(HostDetails hd) {
        final UberHandler.Factory<T> handler = this.handlerFactory();
        final Function<ClusterContext<T>, WriteMarshallable> heartbeat = this.heartbeatFactory();

        @NotNull ArrayList<WriteMarshallable> result = new ArrayList<>();
        result.add(handler.apply(this, hd));
        result.add(heartbeat.apply(this));
        return result;
    }

    public long retryInterval() {
        return retryInterval;
    }

    public ClusterContext<T> retryInterval(final long retryInterval) {
        this.retryInterval = retryInterval;
        return this;
    }
}

