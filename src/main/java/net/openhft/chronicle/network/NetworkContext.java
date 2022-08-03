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

import net.openhft.chronicle.bytes.MappedUniqueTimeProvider;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.ConnectionListeners;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface NetworkContext<T extends NetworkContext<T>> extends Closeable {

    void onHandlerChanged(TcpHandler<T> handler);

    @NotNull
    T isAcceptor(boolean serverSocket);

    boolean isAcceptor();

    @NotNull
    T socketChannel(@NotNull ChronicleSocketChannel sc);

    ChronicleSocketChannel socketChannel();

    WireOutPublisher wireOutPublisher();

    T wireOutPublisher(@NotNull WireOutPublisher wireOutPublisher);

    WireType wireType();

    @NotNull
    T wireType(@NotNull WireType wireType);

    SessionDetailsProvider sessionDetails();

    @NotNull
    T sessionDetails(SessionDetailsProvider sessionDetails);

    long heartbeatTimeoutMs();

    HeartbeatListener heartbeatListener();

    void heartbeatListener(@NotNull HeartbeatListener heartbeatListener);

    T heartbeatTimeoutMs(long l);

    default boolean isUnchecked() {
        return false;
    }

    default long newCid() {
        return MappedUniqueTimeProvider.INSTANCE.currentTimeMicros();
    }

    ServerThreadingStrategy serverThreadingStrategy();

    @Nullable
    default ConnectionListener acquireConnectionListener() {
        return Jvm.isDebug() ? ConnectionListeners.LOGGING : ConnectionListeners.NONE;
    }

    Runnable socketReconnector();

    @NotNull
    T socketReconnector(Runnable socketReconnector);

    void networkStatsListener(@NotNull NetworkStatsListener<T> NetworkStatsListener);

    @Nullable
    NetworkStatsListener<T> networkStatsListener();

    T serverThreadingStrategy(ServerThreadingStrategy singleThreaded);

    default void addConnectionListener(ConnectionListener connectionListener) {
        // do nothing
    }

    default void removeConnectionListener(ConnectionListener connectionListener) {
        // do nothing
    }

    default TimeProvider timeProvider() {
        return SystemTimeProvider.INSTANCE;
    }

    /**
     * Priority for general activities on this context; eg connect, accept, send/recv
     */
    default HandlerPriority priority() {
        return HandlerPriority.MEDIUM;
    }

    /**
     * Priority for periodic activies on this context; eg heartbeats
     */
    default HandlerPriority periodicPriority() {
        return HandlerPriority.TIMER;
    }

    /**
     * Called when the outbound buffer becomes empty (edge-triggered)
     */
    default void onFlushed() {}
}

