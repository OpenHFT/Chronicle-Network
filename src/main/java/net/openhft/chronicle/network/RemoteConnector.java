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
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.NetworkUtil.TCP_BUFFER_SIZE;
import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public class RemoteConnector<T extends NetworkContext<T>> extends SimpleCloseable {

    @NotNull
    private final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpHandlerSupplier;
    private final int tcpBufferSize;
    @NotNull
    private final List<java.io.Closeable> closeables = Collections.synchronizedList(new ArrayList<>());

    public RemoteConnector(@NotNull final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory) {
        this.tcpBufferSize = Jvm.getInteger("tcp.client.buffer.size", TCP_BUFFER_SIZE);
        this.tcpHandlerSupplier = tcpEventHandlerFactory;
    }

    private static void closeSocket(final Closeable socketChannel) {
        closeQuietly(socketChannel);
    }

    public void connect(@NotNull final String remoteHostPort,
                        @NotNull final EventLoop eventLoop,
                        @NotNull T nc,
                        final long retryInterval) {
        throwExceptionIfClosed();
        if (eventLoop instanceof ManagedCloseable)
            ((ManagedCloseable) eventLoop).throwExceptionIfClosed();

        @NotNull final RCEventHandler handler = new RCEventHandler(
                remoteHostPort,
                nc,
                eventLoop,
                retryInterval);

        eventLoop.addHandler(handler);
    }

    @Override
    protected void performClose() {
        closeQuietly(closeables);
        closeables.clear();
    }

    @PackageLocal
    ChronicleSocketChannel openSocketChannel(final InetSocketAddress socketAddress) throws IOException {
        final ChronicleSocketChannel result = ChronicleSocketChannelFactory.wrap(socketAddress);
        result.configureBlocking(false);
        final ChronicleSocket socket = result.socket();
        if (!TcpEventHandler.DISABLE_TCP_NODELAY) socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        socket.setSoTimeout(0);
        socket.setSoLinger(false, 0);
        return result;
    }

    private final class RCEventHandler extends AbstractCloseable implements EventHandler, Closeable {

        private final AtomicLong nextPeriod = new AtomicLong();
        private final String remoteHostPort;
        private final T nc;
        private final EventLoop eventLoop;
        private final long retryInterval;

        RCEventHandler(final String remoteHostPort,
                       final T nc,
                       @NotNull final EventLoop eventLoop,
                       final long retryInterval) {
            this.remoteHostPort = remoteHostPort;
            this.nc = nc;
            this.eventLoop = eventLoop;
            this.retryInterval = retryInterval;
            // add an initial delay to reduce the possibility of successful connecting to a process which is shutting down
            // this does not eliminate the issue, but is rather a tactical work around.
            nextPeriod.set(System.currentTimeMillis() + 500);
        }

        @NotNull
        @Override
        public HandlerPriority priority() {
            return HandlerPriority.BLOCKING;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {
            throwExceptionIfClosed();

            if (isClosed() || eventLoop.isClosing())
                throw InvalidEventHandlerException.reusable();
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

            ChronicleSocketChannel sc = null;
            final TcpEventHandler<T> eventHandler;

            final InetSocketAddress address = TCPRegistry.lookup(remoteHostPort);
            try {
                sc = RemoteConnector.this.openSocketChannel(address);

                if (sc == null)
                    return false;

                if (!sc.isOpen())
                    // this can happen if the acceptor is in the process of shutting down
                    return false;

                nc.socketChannel(sc);
                nc.isAcceptor(false);

                if (!nc.socketChannel().isOpen())
                    return false;

                notifyHostPort(sc, nc.networkStatsListener());

                eventHandler = tcpHandlerSupplier.apply(nc);

            } catch (ClosedIllegalStateException e) {
                Jvm.warn().on(getClass(), "Already closed while connecting to " + address, e);
                // may be already closed by socketReconnector in HostConnector
                closeSocket(sc);

                throw InvalidEventHandlerException.reusable();
            } catch (AlreadyConnectedException | AsynchronousCloseException | UnsupportedAddressTypeException e) {
                Jvm.warn().on(getClass(), "Unable to connect to " + address, e);
                throw InvalidEventHandlerException.reusable();
            } catch (IOException | IORuntimeException | UnresolvedAddressException e) {
                // UnresolvedAddressException: host configuration can change over time, See https://github.com/OpenHFT/Chronicle-Network/issues/136
                nextPeriod.set(System.currentTimeMillis() + retryInterval);
                return false;
            }
            if (isClosed() || eventLoop.isClosing() || Thread.currentThread().isInterrupted())
                // we have died.
                closeQuietly(eventHandler);
            else {
                eventLoop.addHandler(eventHandler);
                closeables.add(() -> closeSocket(nc.socketChannel()));
            }

            throw InvalidEventHandlerException.reusable();
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
            // Do nothing
        }
    }
}