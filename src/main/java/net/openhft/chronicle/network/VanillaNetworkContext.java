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
package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.TerminationEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;

public class VanillaNetworkContext<T extends VanillaNetworkContext<T>> extends AbstractCloseable implements NetworkContext<T> {

    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private HeartbeatListener heartbeatListener;
    private SessionDetailsProvider sessionDetails;
    @Nullable
    private TerminationEventHandler<T> terminationEventHandler;
    private long heartbeatTimeoutMs;
    private WireOutPublisher wireOutPublisher;
    private WireType wireType = WireType.TEXT;
    private Runnable socketReconnector;
    @Nullable
    private NetworkStatsListener<T> networkStatsListener;
    private ServerThreadingStrategy serverThreadingStrategy = ServerThreadingStrategy.SINGLE_THREADED;

    @Override
    public SocketChannel socketChannel() {
        throwExceptionIfClosed();
        return socketChannel;
    }

    @NotNull
    @Override
    public T socketChannel(SocketChannel socketChannel) {
        throwExceptionIfClosed();
        this.socketChannel = socketChannel;
        return (T) this;
    }

    @Override
    public void onHandlerChanged(TcpHandler<T> handler) {
        throwExceptionIfClosed();

    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return this for chaining
     */
    @NotNull
    @Override
    public T isAcceptor(boolean isAcceptor) {
        throwExceptionIfClosed();
        this.isAcceptor = isAcceptor;
        return (T) this;
    }

    /**
     * @return {@code} true if its a server socket, {@code} false if its a client
     */
    @Override
    public boolean isAcceptor() {
        throwExceptionIfClosed();
        return isAcceptor;
    }

    @Override
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public T wireOutPublisher(WireOutPublisher wireOutPublisher) {
        throwExceptionIfClosed();
        this.wireOutPublisher = wireOutPublisher;
        return (T) this;
    }

    @Override
    public WireType wireType() {
        throwExceptionIfClosed();
        return wireType;
    }

    @Override
    @NotNull
    public T wireType(WireType wireType) {
        throwExceptionIfClosed();
        this.wireType = wireType;
        return (T) this;
    }

    @Override
    public SessionDetailsProvider sessionDetails() {
        throwExceptionIfClosed();
        return this.sessionDetails;
    }

    @NotNull
    @Override
    public T sessionDetails(SessionDetailsProvider sessionDetails) {
        throwExceptionIfClosed();
        this.sessionDetails = sessionDetails;
        return (T) this;
    }

    @Nullable
    @Override
    public TerminationEventHandler<T> terminationEventHandler() {
        throwExceptionIfClosed();
        return terminationEventHandler;
    }

    @Override
    public void terminationEventHandler(@Nullable TerminationEventHandler<T> terminationEventHandler) {
        throwExceptionIfClosed();
        this.terminationEventHandler = terminationEventHandler;
    }

    @Override
    public T heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        throwExceptionIfClosed();
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return (T) this;
    }

    @Override
    public long heartbeatTimeoutMs() {
        throwExceptionIfClosed();
        return heartbeatTimeoutMs;
    }

    @Override
    public HeartbeatListener heartbeatListener() {
        throwExceptionIfClosed();
        return this.heartbeatListener;
    }

    @Override
    public void heartbeatListener(@NotNull HeartbeatListener heartbeatListener) {
        throwExceptionIfClosed();
        this.heartbeatListener = heartbeatListener;
    }


    @Override
    protected void performClose() {
        Closeable.closeQuietly(networkStatsListener);
    }

    @Override
    public Runnable socketReconnector() {
        throwExceptionIfClosed();
        return socketReconnector;
    }

    @Override
    @NotNull
    public T socketReconnector(Runnable socketReconnector) {
        throwExceptionIfClosed();
        this.socketReconnector = socketReconnector;
        return (T) this;
    }

    @Override
    public void networkStatsListener(@NotNull NetworkStatsListener<T> networkStatsListener) {
        throwExceptionIfClosed();
        this.networkStatsListener = networkStatsListener;
    }

    @Nullable
    @Override
    public NetworkStatsListener<T> networkStatsListener() {
        return this.networkStatsListener;
    }

    @Override
    public ServerThreadingStrategy serverThreadingStrategy() {
        throwExceptionIfClosed();
        return serverThreadingStrategy;
    }

    @Override
    public T serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        throwExceptionIfClosed();
        this.serverThreadingStrategy = serverThreadingStrategy;
        return (T) this;
    }
}
