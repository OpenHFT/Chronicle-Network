/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.network.connection.FatalFailureMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public interface ConnectionStrategy extends Marshallable, java.io.Closeable {
    long minPauseSec = Integer.getInteger("connectionStrategy.pause.min.secs", 5);
    long maxPauseSec = Integer.getInteger("connectionStrategy.pause.max.secs", 5);


    default ClientConnectionMonitor clientConnectionMonitor() {
        return new VanillaClientConnectionMonitor();
    }

    @Nullable
    static ChronicleSocketChannel socketChannel(@NotNull InetSocketAddress socketAddress, int tcpBufferSize, int socketConnectionTimeoutMs) throws IOException {

        final ChronicleSocketChannel result = ChronicleSocketChannelFactory.wrap();
        @Nullable Selector selector = null;
        boolean failed = true;
        try {
            result.configureBlocking(false);
            ChronicleSocket socket = result.socket();
            if (!TcpEventHandler.DISABLE_TCP_NODELAY) socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(tcpBufferSize);
            socket.setSendBufferSize(tcpBufferSize);
            socket.setSoTimeout(0);
            socket.setSoLinger(false, 0);
            result.connect(socketAddress);

            selector = Selector.open();
            result.register(selector, SelectionKey.OP_CONNECT);

            int select = selector.select(socketConnectionTimeoutMs);
            if (select == 0) {
                if (Jvm.isDebugEnabled(ConnectionStrategy.class))
                    Jvm.debug().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
                return null;
            } else {
                try {
                    if (!result.finishConnect())
                        return null;

                } catch (IOException e) {
                    if (Jvm.isDebugEnabled(ConnectionStrategy.class))
                        Jvm.debug().on(ConnectionStrategy.class, "Failed to connect to " + socketAddress + " " + e);
                    return null;
                }
            }

            failed = false;
            return result;

        } catch (Exception e) {
            return null;
        } finally {
            closeQuietly(selector);
            if (failed)
                closeQuietly(result);
        }
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
     */
    ChronicleSocketChannel connect(@NotNull String name,
                                   @NotNull SocketAddressSupplier socketAddressSupplier,
                                   boolean didLogIn,
                                   @NotNull FatalFailureMonitor fatalFailureMonitor) throws InterruptedException;

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
     */
    @Nullable
    default ChronicleSocketChannel openSocketChannel(@NotNull InetSocketAddress socketAddress,
                                                     int tcpBufferSize,
                                                     long timeoutMs,
                                                     int socketConnectionTimeoutMs) throws IOException, InterruptedException {
        assert timeoutMs > 0;
        long start = System.currentTimeMillis();
        ChronicleSocketChannel sc = socketChannel(socketAddress, tcpBufferSize, socketConnectionTimeoutMs);
        if (sc != null)
            return sc;

        for (; ; ) {
            throwExceptionIfClosed();
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            long startMs = System.currentTimeMillis();
            if (start + timeoutMs < startMs) {
                Jvm.warn().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
                return null;
            }
            sc = socketChannel(socketAddress, tcpBufferSize, socketConnectionTimeoutMs);
            if (sc != null)
                return sc;
            Thread.yield();
            // If nothing is listening, socketChannel returns pretty much immediately so we support a pause here
            pauseBeforeReconnect(startMs);
        }
    }

    default void pauseBeforeReconnect(long startMs) {
        long pauseMillis = (startMs + pauseMillisBeforeReconnect()) - System.currentTimeMillis();
        if (Jvm.isDebugEnabled(this.getClass()))
            Jvm.debug().on(this.getClass(), "Waiting for reconnect " + pauseMillis + " ms");
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(pauseMillis));
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
        return minPauseSec;
    }

    default long maxPauseSec() {
        return maxPauseSec;
    }
}
