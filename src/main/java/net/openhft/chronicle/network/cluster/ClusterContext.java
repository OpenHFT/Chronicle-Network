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

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Rob Austin.
 */
public class ClusterContext implements Demarshallable, WriteMarshallable, Consumer<HostDetails> {

    private ConnectionStrategy connectionStrategy;
    private WireType wireType;
    private BiFunction<ClusterContext, HostDetails, WriteMarshallable> handlerFactory;
    private Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private Function<ClusterContext, NetworkContext> networkContextFactory;
    private Supplier<ConnectionManager> connectionEventHandler;
    private long heartbeatTimeoutMs = 40_000;
    private long heartbeatIntervalTicks = 20_000;
    private String clusterName;
    private EventLoop eventLoop;
    private Function<ClusterContext, WriteMarshallable> heartbeatFactory;

    protected ClusterContext(@NotNull WireIn wire) {
        while (wire.bytes().readRemaining() > 0)
            wireParser().parseOne(wire, null);
    }

    public ClusterContext(WireType wireType,
                          BiFunction<ClusterContext, HostDetails, WriteMarshallable> handlerFactory,
                          Function<WireType, WireOutPublisher> wireOutPublisherFactory,
                          Function<ClusterContext, NetworkContext> networkContextFactory, Supplier<ConnectionManager> connectionEventHandler,
                          long heartbeatTimeoutMs,
                          long heartbeatIntervalTicks,
                          String clusterName,
                          EventLoop eventLoop,
                          Function<ClusterContext, WriteMarshallable> heartbeatFactory,
                          ConnectionStrategy connectionStrategy,
                          byte localIdentifier,
                          long connectionTimeoutMs) {

        this.wireType = wireType;
        this.handlerFactory = handlerFactory;
        this.wireOutPublisherFactory = wireOutPublisherFactory;
        this.networkContextFactory = networkContextFactory;
        this.connectionEventHandler = connectionEventHandler;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        this.clusterName = clusterName;
        this.eventLoop = eventLoop;
        this.heartbeatFactory = heartbeatFactory;
        this.connectionStrategy = connectionStrategy;
        this.localIdentifier = localIdentifier;
        this.connectionTimeoutMs = connectionTimeoutMs;
    }


    public Function<NetworkContext, TcpEventHandler> tcpEventHandlerFactory() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    public WireParser<Void> wireParser() {
        WireParser<Void> parser = new VanillaWireParser<>((s, v, $) -> {
        });
        parser.register(() -> "wireType", (s, v, $) -> v.text(this, (o, x) -> this.wireType(WireType.valueOf(x))));
        parser.register(() -> "handlerFactory", (s, v, $) -> this.handlerFactory(v.typedMarshallable()));
        parser.register(() -> "heartbeatTimeoutMs", (s, v, $) -> this.heartbeatTimeoutMs(v.int64()));
        parser.register(() -> "heartbeatIntervalTicks", (s, v, $) -> this.heartbeatIntervalTicks(v.int64()));
        parser.register(() -> "wireOutPublisherFactory", (s, v, $) -> this.wireOutPublisherFactory(v.typedMarshallable()));
        parser.register(() -> "networkContextFactory", (s, v, $) -> this.networkContextFactory(v.typedMarshallable()));
        parser.register(() -> "connectionTimeoutMs", (s, v, $) -> this.connectionTimeoutMs(v.int64
                ()));
        parser.register(() -> "connectionStrategy", (s, v, $) -> this.connectionStrategy(v.typedMarshallable
                ()));
        parser.register(() -> "connectionEventHandler", (s, v, $) -> this.connectionEventHandler(v.typedMarshallable
                ()));

        parser.register(() -> "heartbeatFactory", (s, v, $) -> this.heartbeatFactory(v.typedMarshallable
                ()));
        return parser;
    }


    public BiFunction<ClusterContext, HostDetails, WriteMarshallable> handlerFactory() {
        return handlerFactory;
    }

    public void handlerFactory(BiFunction<ClusterContext, HostDetails, WriteMarshallable> handlerFactory) {
        this.handlerFactory = handlerFactory;
    }

    public void clusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }


    public ClusterContext eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    byte localIdentifier;
    long connectionTimeoutMs = 1_000;

    public ClusterContext() {
        defaults();
    }

    public void defaults() {
    }

    public ClusterContext localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
        return this;
    }


    public ClusterContext wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }


    public ClusterContext heartbeatFactory(Function<ClusterContext, WriteMarshallable> heartbeatFactor) {
        this.heartbeatFactory = heartbeatFactor;
        return this;
    }


    public ClusterContext heartbeatIntervalTicks(long heartbeatIntervalTicks) {
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        return this;
    }

    public long heartbeatIntervalTicks() {
        return this.heartbeatIntervalTicks;
    }

    public ClusterContext heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return this;
    }

    public ClusterContext wireOutPublisherFactory(Function<WireType, WireOutPublisher> wireOutPublisherFactory) {
        this.wireOutPublisherFactory = wireOutPublisherFactory;
        return this;
    }

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


    public ClusterContext connectionTimeoutMs(long connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    public Function<ClusterContext, NetworkContext> networkContextFactory() {
        return networkContextFactory;
    }

    public long connectionTimeoutMs() {
        return this.connectionTimeoutMs;
    }

    public ClusterContext connectionStrategy(ConnectionStrategy connectionStrategy) {
        this.connectionStrategy = connectionStrategy;
        return this;
    }

    public ConnectionStrategy connectionStrategy() {
        return this.connectionStrategy;
    }

    public Supplier<ConnectionManager> connectionEventHandler() {
        return connectionEventHandler;
    }

    public ClusterContext connectionEventHandler(Supplier<ConnectionManager> connectionEventHandler) {
        this.connectionEventHandler = connectionEventHandler;
        return this;
    }

    public Function<ClusterContext, WriteMarshallable> heartbeatFactory() {
        return heartbeatFactory;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void accept(@NotNull HostDetails hd) {
        if (this.localIdentifier == hd.hostId())
            return;

        final ConnectionStrategy connectionStrategy = this.connectionStrategy();
        hd.connectionStrategy(connectionStrategy);

        final ConnectionManager eventManagerHandler = this
                .connectionEventHandler().get();
        hd.connectionEventManagerHandler(eventManagerHandler);

        final HostConnector hostConnector = new HostConnector(this, new
                RemoteConnector(this.tcpEventHandlerFactory()), hd);

        hd.hostConnector(hostConnector);

        ClusterNotifier clusterHandler = new ClusterNotifier(eventManagerHandler,
                hostConnector, bootstraps(hd));

        hd.clusterHandler(clusterHandler);
        hd.terminationEventHandler(clusterHandler);

        clusterHandler.connect();
    }


    private List<WriteMarshallable> bootstraps(HostDetails hd) {
        final BiFunction<ClusterContext, HostDetails, WriteMarshallable> handler = this
                .handlerFactory();
        final Function<ClusterContext, WriteMarshallable> heartbeat = this.heartbeatFactory();

        ArrayList<WriteMarshallable> result = new ArrayList<WriteMarshallable>();
        result.add(handler.apply(this, hd));
        result.add(heartbeat.apply(this));
        return result;
    }

}
