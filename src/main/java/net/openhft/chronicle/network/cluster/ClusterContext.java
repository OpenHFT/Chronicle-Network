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

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.cluster.handlers.UberHandler.Factory;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class ClusterContext implements Demarshallable, Marshallable, Consumer<HostDetails> {

    private transient Factory handlerFactory;
    private transient Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private transient Function<ClusterContext, NetworkContext> networkContextFactory;
    private transient Function<ClusterContext, WriteMarshallable> heartbeatFactory;
    private transient Function<ClusterContext, NetworkStatsListener> networkStatsListenerFactory;
    private transient Supplier<ConnectionManager> connectionEventHandler;
    private transient EventLoop eventLoop;
    private long heartbeatTimeoutMs = 40_000;
    private long heartbeatIntervalMs = 20_000;
    private ConnectionStrategy connectionStrategy;
    private WireType wireType;
    private String clusterName;
    private byte localIdentifier;
    private ServerThreadingStrategy serverThreadingStrategy;
    private long retryInterval = 1_000L;
    private String procPrefix;

    @UsedViaReflection
    protected ClusterContext(@NotNull WireIn wire) throws IORuntimeException {
        readMarshallable(wire);
    }

    protected ClusterContext() {
        defaults();
    }

    public String procPrefix() {
        return procPrefix;
    }

    public void procPrefix(String procPrefix) {
        this.procPrefix = procPrefix;
    }


    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        defaults();
        while (wire.bytes().readRemaining() > 0) {
            wire.consumePadding();
            if (wire.bytes().readRemaining() > 0)
                wireParser().parseOne(wire);
        }
    }

    public Function<ClusterContext, NetworkStatsListener> networkStatsListenerFactory() {
        return networkStatsListenerFactory;
    }

    @NotNull
    public ClusterContext networkStatsListenerFactory(Function<ClusterContext, NetworkStatsListener> networkStatsListenerFactory) {
        this.networkStatsListenerFactory = networkStatsListenerFactory;
        return this;
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    @NotNull
    public abstract ThrowingFunction<NetworkContext, TcpEventHandler, IOException> tcpEventHandlerFactory();

    @NotNull
    protected WireParser wireParser() {
        @NotNull VanillaWireParser parser = new VanillaWireParser((s, v) -> {
        }, WireParser.SKIP_READABLE_BYTES);
        parser.register(() -> "wireType", (s, v) -> v.text(this, (o, x) -> this.wireType(WireType.valueOf(x))));
        parser.register(() -> "handlerFactory", (s, v) -> this.handlerFactory(v.typedMarshallable()));
        parser.register(() -> "heartbeatTimeoutMs", (s, v) -> this.heartbeatTimeoutMs(v.int64()));
        parser.register(() -> "heartbeatIntervalMs", (s, v) -> this.heartbeatIntervalMs(v.int64()));
        parser.register(() -> "wireOutPublisherFactory", (s, v) -> this.wireOutPublisherFactory(v.typedMarshallable()));
        parser.register(() -> "networkContextFactory", (s, v) -> this.networkContextFactory(v.typedMarshallable()));
        parser.register(() -> "connectionStrategy", (s, v) -> this.connectionStrategy(v.typedMarshallable()));
        parser.register(() -> "connectionEventHandler", (s, v) -> this.connectionEventHandler(v.typedMarshallable()));
        parser.register(() -> "heartbeatFactory", (s, v) -> this.heartbeatFactory(v.typedMarshallable()));
        parser.register(() -> "networkStatsListenerFactory", (s, v) -> this.networkStatsListenerFactory(v.typedMarshallable()));
        parser.register(() -> "serverThreadingStrategy", (s, v) -> this.serverThreadingStrategy(v.asEnum(ServerThreadingStrategy.class)));
        return parser;
    }

    public void serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        this.serverThreadingStrategy = serverThreadingStrategy;
    }

    public ServerThreadingStrategy serverThreadingStrategy() {
        return serverThreadingStrategy;
    }

    private UberHandler.Factory handlerFactory() {
        return handlerFactory;
    }

    public void handlerFactory(UberHandler.Factory handlerFactory) {
        this.handlerFactory = handlerFactory;
    }

    public void clusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }

    @NotNull
    public ClusterContext eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    public void defaults() {
    }

    @NotNull
    public ClusterContext localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
        return this;
    }

    @NotNull
    public ClusterContext wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    @NotNull
    public ClusterContext heartbeatFactory(Function<ClusterContext, WriteMarshallable>
                                                   heartbeatFactor) {
        this.heartbeatFactory = heartbeatFactor;
        return this;
    }

    @NotNull
    public ClusterContext heartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return this;
    }

    @NotNull
    public ClusterContext heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return this;
    }

    @NotNull
    public ClusterContext wireOutPublisherFactory(Function<WireType, WireOutPublisher> wireOutPublisherFactory) {
        this.wireOutPublisherFactory = wireOutPublisherFactory;
        return this;
    }

    @NotNull
    public ClusterContext networkContextFactory(Function<ClusterContext, NetworkContext> networkContextFactory) {
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

    public Function<ClusterContext, NetworkContext> networkContextFactory() {
        return networkContextFactory;
    }

    @NotNull
    public ClusterContext connectionStrategy(ConnectionStrategy connectionStrategy) {
        this.connectionStrategy = connectionStrategy;
        return this;
    }

    private ConnectionStrategy connectionStrategy() {
        return this.connectionStrategy;
    }

    private Supplier<ConnectionManager> connectionEventHandler() {
        return connectionEventHandler;
    }

    @NotNull
    public ClusterContext connectionEventHandler(Supplier<ConnectionManager> connectionEventHandler) {
        this.connectionEventHandler = connectionEventHandler;
        return this;
    }

    private Function<ClusterContext, WriteMarshallable> heartbeatFactory() {
        return heartbeatFactory;
    }

    @Override
    public void accept(@NotNull HostDetails hd) {
        if (this.localIdentifier == hd.hostId())
            return;

        final ConnectionStrategy connectionStrategy = this.connectionStrategy();
        hd.connectionStrategy(connectionStrategy);

        final ConnectionManager connectionManager = this
                .connectionEventHandler().get();
        hd.connectionManager(connectionManager);

        @NotNull final HostConnector hostConnector = new HostConnector(this, new
                RemoteConnector(this.tcpEventHandlerFactory()), hd);

        hd.hostConnector(hostConnector);

        @NotNull ClusterNotifier clusterNotifier = new ClusterNotifier(connectionManager,
                hostConnector, bootstraps(hd));

        hd.clusterNotifier(clusterNotifier);
        hd.terminationEventHandler(clusterNotifier);

        clusterNotifier.connect();
    }

    @NotNull
    private List<WriteMarshallable> bootstraps(HostDetails hd) {
        final UberHandler.Factory handler = this.handlerFactory();
        final Function<ClusterContext, WriteMarshallable> heartbeat = this.heartbeatFactory();

        @NotNull ArrayList<WriteMarshallable> result = new ArrayList<>();
        result.add(handler.apply(this, hd));
        result.add(heartbeat.apply(this));
        return result;
    }

    public long retryInterval() {
        return retryInterval;
    }

    public ClusterContext retryInterval(final long retryInterval) {
        this.retryInterval = retryInterval;
        return this;
    }
}

