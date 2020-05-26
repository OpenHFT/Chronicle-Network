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
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public class AcceptorEventHandler<T extends NetworkContext<T>> extends AbstractCloseable implements EventHandler, Closeable {
    @NotNull
    private final Function<T, TcpEventHandler<T>> handlerFactory;
    @NotNull

    private final ServerSocketChannel ssc;
    @NotNull
    private final Supplier<T> ncFactory;
    private final String hostPort;
    private final AcceptStrategy acceptStrategy;

    private EventLoop eventLoop;

    public AcceptorEventHandler(@NotNull final String hostPort,
                                @NotNull final Function<T, TcpEventHandler<T>> handlerFactory,
                                @NotNull final Supplier<T> ncFactory) throws IOException {
        this(hostPort, handlerFactory, ncFactory, AcceptStrategy.ACCEPT_ALL);
    }

    public AcceptorEventHandler(@NotNull final String hostPort,
                                @NotNull final Function<T, TcpEventHandler<T>> handlerFactory,
                                @NotNull final Supplier<T> ncFactory,
                                @NotNull final AcceptStrategy acceptStrategy) throws IOException {
        this.handlerFactory = handlerFactory;
        this.hostPort = hostPort;
        this.ssc = TCPRegistry.acquireServerSocketChannel(this.hostPort);
        this.ncFactory = ncFactory;
        this.acceptStrategy = acceptStrategy;
    }

    @Override
    public void eventLoop(final EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        if (!ssc.isOpen() || isClosed() || eventLoop.isClosed())
            throw new InvalidEventHandlerException();

        try {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "accepting " + ssc);

            final SocketChannel sc = acceptStrategy.accept(ssc);

            if (sc != null) {
                if (isClosed() || eventLoop.isClosed()) {
                    Closeable.closeQuietly(sc);
                    throw new InvalidEventHandlerException("closed");
                }
                final T nc = ncFactory.get();
                nc.socketChannel(sc);
                nc.isAcceptor(true);
                NetworkStatsListener<T> nl = nc.networkStatsListener();
                notifyHostPort(sc, nl);
                TcpEventHandler<T> apply = handlerFactory.apply(nc);
                eventLoop.addHandler(apply);
            }
        } catch (AsynchronousCloseException e) {
            closeSocket();
            throw new InvalidEventHandlerException(e);
        } catch (ClosedChannelException e) {
            Jvm.debug().on(this.getClass(), "ClosedChannel");
            closeSocket();
            if (isClosed())
                throw new InvalidEventHandlerException();
            else
                throw new InvalidEventHandlerException(e);
        } catch (
                Exception e) {

            if (!isClosed() && !eventLoop.isClosed()) {
                final ServerSocket socket = ssc.socket();
                if (socket != null)
                    Jvm.warn().on(getClass(), hostPort + ", port=" + socket.getLocalPort(), e);
                else
                    Jvm.warn().on(getClass(), hostPort, e);
            }
            closeSocket();
            throw new InvalidEventHandlerException(e);
        }
        return false;
    }

    private void closeSocket() {
        try {
            ssc.socket().close();
        } catch (IOException e) {
            Jvm.debug().on(getClass(), e);
        }

        try {
            ssc.close();
        } catch (IOException e) {
            Jvm.debug().on(getClass(), e);
        }
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    protected void performClose() {
        closeSocket();
    }
}
