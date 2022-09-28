/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class VanillaNetworkContext<T extends NetworkContext<T>> extends AbstractCloseable implements NetworkContext<T> {

    private ChronicleSocketChannel socketChannel;
    private boolean isAcceptor = true;
    private HeartbeatListener heartbeatListener;
    private SessionDetailsProvider sessionDetails;
    private long heartbeatTimeoutMs;
    private WireOutPublisher wireOutPublisher;
    private WireType wireType = WireType.TEXT;
    private Runnable socketReconnector;
    @Nullable
    private NetworkStatsListener<T> networkStatsListener;

    @Deprecated(/* To be considered for removal in x.25 */)
    private ServerThreadingStrategy serverThreadingStrategy = ServerThreadingStrategy.SINGLE_THREADED;

    public VanillaNetworkContext() {
        singleThreadedCheckDisabled(true);
    }

    @Override
    public ChronicleSocketChannel socketChannel() {
        return socketChannel;
    }

    /**
     * We don't perform close in background because delaying it would mean
     * the background resource releaser would end up calling close on the
     * network stats listener. The FixNetworkContext also had some listeners
     * that were being called after the fact by background resource releaser.
     *
     * see https://github.com/ChronicleEnterprise/Chronicle-FIX/issues/716
     *
     * The {@link net.openhft.chronicle.network.tcp.VanillaSocketChannel}
     * is released in the background itself and the {@link WireOutPublisher}
     * could be made to do so if it's slow
     *
     * @return false
     */
    @Override
    protected boolean shouldPerformCloseInBackground() {
        return false;
    }

    @NotNull
    @Override
    public T socketChannel(@NotNull ChronicleSocketChannel socketChannel) {
        throwExceptionIfClosedInSetter();

        this.socketChannel = socketChannel;
        return self();
    }

    @Override
    public void onHandlerChanged(TcpHandler<T> handler) {
        // Do nothing
    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return this for chaining
     */
    @NotNull
    @Override
    public T isAcceptor(boolean isAcceptor) {
        throwExceptionIfClosedInSetter();

        this.isAcceptor = isAcceptor;
        return self();
    }

    /**
     * @return {@code} true if it is a server socket, {@code} false if it is a client
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
    public T wireOutPublisher(@NotNull WireOutPublisher wireOutPublisher) {
        throwExceptionIfClosedInSetter();

        this.wireOutPublisher = wireOutPublisher;
        return self();
    }

    @Override
    public WireType wireType() {
        return wireType;
    }

    @Override
    @NotNull
    public T wireType(@NotNull WireType wireType) {
        throwExceptionIfClosedInSetter();

        this.wireType = wireType;
        return self();
    }

    @Override
    public SessionDetailsProvider sessionDetails() {
        return this.sessionDetails;
    }

    @NotNull
    @Override
    public T sessionDetails(SessionDetailsProvider sessionDetails) {
        throwExceptionIfClosedInSetter();

        this.sessionDetails = sessionDetails;
        return self();
    }

    @Override
    public T heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        throwExceptionIfClosedInSetter();

        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return self();
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
        throwExceptionIfClosedInSetter();

        this.heartbeatListener = heartbeatListener;
    }

    @Override
    protected void performClose() {
        closeQuietly(socketChannel, wireOutPublisher, networkStatsListener);
    }

    @Override
    public Runnable socketReconnector() {
        return socketReconnector;
    }

    @Override
    @NotNull
    public T socketReconnector(Runnable socketReconnector) {
        throwExceptionIfClosedInSetter();

        // if already set, close it first
        closeQuietly(this.socketReconnector);

        this.socketReconnector = socketReconnector;
        return self();
    }

    @Override
    public void networkStatsListener(@NotNull NetworkStatsListener<T> networkStatsListener) {
        throwExceptionIfClosedInSetter();

        this.networkStatsListener = networkStatsListener;
    }

    @Nullable
    @Override
    public NetworkStatsListener<T> networkStatsListener() {
        return this.networkStatsListener;
    }

    @Deprecated(/* To be considered for removal in x.25 */)
    @Override
    public ServerThreadingStrategy serverThreadingStrategy() {
        return serverThreadingStrategy;
    }

    @Deprecated(/* To be considered for removal in x.25 */)
    @Override
    public T serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        throwExceptionIfClosedInSetter();

        this.serverThreadingStrategy = serverThreadingStrategy;
        return self();
    }

    private T self() {
        return (T) this;
    }
}
