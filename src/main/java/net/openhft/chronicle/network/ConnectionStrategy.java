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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.network.connection.*;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import static net.openhft.chronicle.network.connection.ConnectionState.ESTABLISHED;
import static net.openhft.chronicle.network.connection.ConnectionState.FAILED;

public interface ConnectionStrategy extends Marshallable, java.io.Closeable {

    default ClientConnectionMonitor clientConnectionMonitor() {
        return new VanillaClientConnectionMonitor();
    }

    /**
     * @deprecated Use {@link ChronicleSocketChannel#builder(InetSocketAddress)} instead
     */
    @Deprecated(/* To be removed in x.26 */)
    @Nullable
    static ChronicleSocketChannel socketChannel(@NotNull InetSocketAddress socketAddress, int tcpBufferSize, int socketConnectionTimeoutMs) throws IOException {
        return ChronicleSocketChannel.builder(socketAddress)
                .tcpBufferSize(tcpBufferSize)
                .socketConnectionTimeoutMs(socketConnectionTimeoutMs)
                .open();
    }

    /**
     * Connects and returns a new SocketChannel.
     *
     * @param name                  the name of the connection, only used for logging
     * @param socketAddressSupplier to use for address
     * @param didLogIn              was the last attempt successful, was a login established
     * @param fatalFailureMonitor   this is invoked on failures
     * @return the SocketChannel
     * @throws InterruptedException if the channel is interrupted.
     * @deprecated Use {@link #connect(String, SocketAddressSupplier, ConnectionState, FatalFailureMonitor)} instead
     */
    @Deprecated(/* For removal in x.25 */)
    default ChronicleSocketChannel connect(@NotNull String name,
                                           @NotNull SocketAddressSupplier socketAddressSupplier,
                                           boolean didLogIn,
                                           @NotNull FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {
        return connect(name, socketAddressSupplier, didLogIn ? ESTABLISHED : FAILED, fatalFailureMonitor);
    }

    /**
     * Connects and returns a new SocketChannel.
     *
     * @param name                    the name of the connection, only used for logging
     * @param socketAddressSupplier   to use for address
     * @param previousConnectionState the state the previous connection reached
     * @param fatalFailureMonitor     this is invoked on failures
     * @return the SocketChannel
     * @throws InterruptedException if the channel is interrupted.
     */
    default ChronicleSocketChannel connect(@NotNull String name,
                                           @NotNull SocketAddressSupplier socketAddressSupplier,
                                           @NotNull ConnectionState previousConnectionState,
                                           @NotNull FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {
        return connect(name, socketAddressSupplier, previousConnectionState == ESTABLISHED, fatalFailureMonitor);
    }

    /**
     * @deprecated Moved to {@link net.openhft.chronicle.network.connection.OpenSocketStrategy}
     */
    @Deprecated(/* To be removed in x.25 */)
    @Nullable
    default ChronicleSocketChannel openSocketChannel(@NotNull InetSocketAddress socketAddress,
                                                     int tcpBufferSize,
                                                     long timeoutMs) throws IOException, InterruptedException {

        return openSocketChannel(socketAddress,
                tcpBufferSize,
                timeoutMs,
                1);
    }

    /**
     * the reason for this method is that unlike the selector it uses tick time
     *
     * @deprecated Moved to {@link net.openhft.chronicle.network.connection.OpenSocketStrategy#openSocketChannel(ConnectionStrategy, InetSocketAddress, int, long, int)}
     */
    @Deprecated(/* To be removed in x.25 */)
    @Nullable
    default ChronicleSocketChannel openSocketChannel(@NotNull InetSocketAddress socketAddress,
                                                     int tcpBufferSize,
                                                     long timeoutMs,
                                                     int socketConnectionTimeoutMs) throws IOException, InterruptedException {
        return DefaultOpenSocketStrategy.INSTANCE.openSocketChannel(this, socketAddress, tcpBufferSize, timeoutMs, socketConnectionTimeoutMs);
    }

    /**
     * allows control of a backoff strategy
     *
     * @return how long in milliseconds to pause before attempting a reconnect
     */
    default long pauseMillisBeforeReconnect() {
        return 500;
    }

    @Override
    default void close() {
    }

    default boolean isClosed() {
        return false;
    }

    default ConnectionStrategy open() {
        return this;
    }

    default void throwExceptionIfClosed() throws IllegalStateException {
        if (this.isClosed()) {
            throw new ClosedIllegalStateException("Closed");
        }
    }

    default long minPauseSec() {
        return Jvm.getInteger("connectionStrategy.pause.min.secs", 5);
    }

    default long maxPauseSec() {
        return Jvm.getInteger("connectionStrategy.pause.max.secs", 5);
    }

    /**
     * Get the local socket to bind the connection to
     *
     * @return the local socket to bind to or null to not bind to any local socket
     * @throws SocketException       If an I/O error occurs
     * @throws UnknownHostException  If an adddress cannot be resolved
     * @throws IllegalStateException If a hostname or interface has no matching addresses
     */
    @Nullable
    default InetSocketAddress localSocketBinding() throws SocketException, UnknownHostException, IllegalStateException {
        return null;
    }
}
