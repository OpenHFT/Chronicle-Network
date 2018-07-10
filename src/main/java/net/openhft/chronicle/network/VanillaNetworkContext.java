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

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.TerminationEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T>, Closeable {

    private final AtomicLong cid = new AtomicLong();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private HeartbeatListener heartbeatListener;
    private SessionDetailsProvider sessionDetails;
    @Nullable
    private TerminationEventHandler terminationEventHandler;
    private long heartbeatTimeoutMs;
    private WireOutPublisher wireOutPublisher;
    private WireType wireType = WireType.TEXT;
    private Runnable socketReconnector;
    @Nullable
    private NetworkStatsListener<? extends NetworkContext> networkStatsListener;
    private ServerThreadingStrategy serverThreadingStrategy = ServerThreadingStrategy.SINGLE_THREADED;

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @NotNull
    @Override
    public T socketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        return (T) this;
    }

    @Override
    public void onHandlerChanged(TcpHandler handler) {

    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return
     */
    @NotNull
    @Override
    public T isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
        return (T) this;
    }

    /**
     * @return {@code} true if its a server socket, {@code} false if its a client
     */
    @Override
    public boolean isAcceptor() {
        return isAcceptor;
    }

    @Override
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public void wireOutPublisher(WireOutPublisher wireOutPublisher) {
        this.wireOutPublisher = wireOutPublisher;
    }

    @Override
    public WireType wireType() {
        return wireType;
    }

    @Override
    @NotNull
    public T wireType(WireType wireType) {
        this.wireType = wireType;
        return (T) this;
    }

    @Override
    public SessionDetailsProvider sessionDetails() {
        return this.sessionDetails;
    }

    @NotNull
    @Override
    public T sessionDetails(SessionDetailsProvider sessionDetails) {
        this.sessionDetails = sessionDetails;
        return (T) this;
    }

    @Nullable
    @Override
    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    @Override
    public void terminationEventHandler(@Nullable TerminationEventHandler terminationEventHandler) {
        this.terminationEventHandler = terminationEventHandler;
    }

    @Override
    public T heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return (T) this;
    }

    @Override
    public long heartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    @Override
    public HeartbeatListener heartbeatListener() {
        return this.heartbeatListener;
    }

    @Override
    public void heartbeatListener(@NotNull HeartbeatListener heartbeatListener) {
        this.heartbeatListener = heartbeatListener;
    }

    @Override
    public long newCid() {

        long time = System.nanoTime();

        for (; ; ) {
            long value = cid.get();
            if (time <= value) {
                time = value + 1;
                continue;
            }

            if (cid.compareAndSet(value, time)) {
                return time;
            }
        }
    }

    @Override
    public void close() {
        closeAtomically();
    }

    /**
     * Close the connection atomically.
     *
     * @return true if state changed to closed; false if nothing changed.
     */
    protected boolean closeAtomically() {
        if (isClosed.compareAndSet(false, true)) {
            Closeable.closeQuietly(networkStatsListener);
            return true;
        } else {
            //was already closed.
            return false;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public Runnable socketReconnector() {
        return socketReconnector;
    }

    @Override
    @NotNull
    public T socketReconnector(Runnable socketReconnector) {
        this.socketReconnector = socketReconnector;
        return (T) this;
    }

    @Override
    public void networkStatsListener(@NotNull NetworkStatsListener networkStatsListener) {
        this.networkStatsListener = networkStatsListener;
    }

    @Nullable
    @Override
    public NetworkStatsListener<? extends NetworkContext> networkStatsListener() {
        return this.networkStatsListener;
    }

    @Override
    public ServerThreadingStrategy serverThreadingStrategy() {
        return serverThreadingStrategy;
    }

    @Override
    public void serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        this.serverThreadingStrategy = serverThreadingStrategy;
    }
}
