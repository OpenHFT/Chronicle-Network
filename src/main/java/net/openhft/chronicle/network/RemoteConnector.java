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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public class RemoteConnector<T extends NetworkContext<T>> extends AbstractCloseable implements Closeable {

    @NotNull
    private final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpHandlerSupplier;

    private final Integer tcpBufferSize;

    @NotNull
    private final List<java.io.Closeable> closeables = new ArrayList<>();

    public RemoteConnector(@NotNull final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory) {
        this.tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", TcpChannelHub.TCP_BUFFER);
        this.tcpHandlerSupplier = tcpEventHandlerFactory;
    }

    private static void closeSocket(SocketChannel socketChannel) {
        Closeable.closeQuietly(socketChannel);
    }

    public void connect(@NotNull final String remoteHostPort,
                        @NotNull final EventLoop eventLoop,
                        @NotNull T nc,
                        final long retryInterval) {

        final InetSocketAddress address = TCPRegistry.lookup(remoteHostPort);

        @NotNull final RCEventHandler handler = new RCEventHandler(
                remoteHostPort,
                nc,
                eventLoop,
                address, retryInterval);

        eventLoop.addHandler(handler);
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(closeables);
    }

    @PackageLocal
    SocketChannel openSocketChannel(InetSocketAddress socketAddress) throws IOException {
        final SocketChannel result = SocketChannel.open(socketAddress);
        result.configureBlocking(false);
        Socket socket = result.socket();
        if (!TcpEventHandler.DISABLE_TCP_NODELAY) socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        socket.setSoTimeout(0);
        socket.setSoLinger(false, 0);
        return result;
    }

    private class RCEventHandler extends AbstractCloseable implements EventHandler, Closeable {

        private final InetSocketAddress address;
        private final AtomicLong nextPeriod = new AtomicLong();
        private final String remoteHostPort;
        private final T nc;
        private final EventLoop eventLoop;
        private final long retryInterval;

        RCEventHandler(String remoteHostPort,
                       T nc,
                       EventLoop eventLoop,
                       InetSocketAddress address, long retryInterval) {
            this.remoteHostPort = remoteHostPort;
            this.nc = nc;
            this.eventLoop = eventLoop;
            this.address = address;
            this.retryInterval = retryInterval;
        }

        @NotNull
        @Override
        public HandlerPriority priority() {
            return HandlerPriority.BLOCKING;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (isClosed() || eventLoop.isClosed())
                throw new InvalidEventHandlerException();
            final long time = System.currentTimeMillis();

            if (time > nextPeriod.get()) {
                nextPeriod.set(time + retryInterval);
            } else {
                // this is called in a loop from BlockingEventHandler,
                // so just wait until the end of the retryInterval
                if (priority() == HandlerPriority.BLOCKING) {
                    final long sleepMillis = nextPeriod.get() - time;
                    if (sleepMillis > 10) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(sleepMillis - 10));
                    }
                }
                return false;
            }

            final SocketChannel sc;
            final TcpEventHandler<T> eventHandler;

            try {
                sc = RemoteConnector.this.openSocketChannel(address);

                if (sc == null)
                    return false;

                nc.socketChannel(sc);
                nc.isAcceptor(false);
                notifyHostPort(sc, nc.networkStatsListener());
                if (!nc.socketChannel().isOpen())
                    throw new InvalidEventHandlerException();
                eventHandler = tcpHandlerSupplier.apply(nc);

            } catch (AlreadyConnectedException e) {
                Jvm.debug().on(getClass(), e);
                throw new InvalidEventHandlerException();
            } catch (IOException | IORuntimeException e) {
                nextPeriod.set(System.currentTimeMillis() + retryInterval);
                return false;
            }
            if (isClosed() || eventLoop.isClosed() || Thread.currentThread().isInterrupted())
                // we have died.
                Closeable.closeQuietly(eventHandler);
            else {
                eventLoop.addHandler(eventHandler);
                closeables.add(() -> closeSocket(sc));
            }

            throw new InvalidEventHandlerException();
        }

        @NotNull
        @Override
        public String toString() {
            return getClass().getSimpleName() + "{"
                    + "remoteHostPort=" + remoteHostPort
                    + ", closed=" + isClosed() +
                    "}";
        }

        @Override
        protected void performClose() {
        }

        @Override
        public void notifyClosing() {
            close();
        }
    }
}