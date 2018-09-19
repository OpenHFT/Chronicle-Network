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

public interface NetworkContext<T extends NetworkContext> extends Closeable {

    void onHandlerChanged(TcpHandler handler);

    @NotNull
    T isAcceptor(boolean serverSocket);

    boolean isAcceptor();

    @NotNull
    T socketChannel(SocketChannel sc);

    SocketChannel socketChannel();

    WireOutPublisher wireOutPublisher();

    void wireOutPublisher(WireOutPublisher wireOutPublisher);

    WireType wireType();

    @NotNull
    T wireType(WireType wireType);

    SessionDetailsProvider sessionDetails();

    @NotNull
    T sessionDetails(SessionDetailsProvider sessionDetails);

    @Nullable
    TerminationEventHandler terminationEventHandler();

    void terminationEventHandler(TerminationEventHandler terminationEventHandler);

    long heartbeatTimeoutMs();

    HeartbeatListener heartbeatListener();

    void heartbeatListener(HeartbeatListener heartbeatListener);

    T heartbeatTimeoutMs(long l);

    default boolean isUnchecked() {
        return false;
    }

    long newCid();

    ServerThreadingStrategy serverThreadingStrategy();

    @Nullable
    default ConnectionListener acquireConnectionListener() {
        return new ConnectionListener() {
            @Override
            public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {

            }

            @Override
            public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {

            }
        };
    }

    Runnable socketReconnector();

    @NotNull
    T socketReconnector(Runnable socketReconnector);

    void networkStatsListener(NetworkStatsListener NetworkStatsListener);

    @Nullable
    NetworkStatsListener networkStatsListener();

    void serverThreadingStrategy(ServerThreadingStrategy singleThreaded);

    default void addConnectionListener(ConnectionListener connectionListener) {
        // do nothing
    }
}

