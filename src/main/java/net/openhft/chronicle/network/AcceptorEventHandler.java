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
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.tcp.ChronicleServerSocket;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public class AcceptorEventHandler<T extends NetworkContext<T>> extends AbstractCloseable implements EventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptorEventHandler.class);
    @NotNull
    private final Function<T, TcpEventHandler<T>> handlerFactory;
    @NotNull

    private final ChronicleServerSocketChannel ssc;
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

    public AcceptorEventHandler(@NotNull final ChronicleServerSocketChannel ssc,
                                @NotNull final Function<T, TcpEventHandler<T>> handlerFactory,
                                @NotNull final Supplier<T> ncFactory,
                                @NotNull final AcceptStrategy acceptStrategy) throws IOException {
        this.handlerFactory = handlerFactory;
        this.hostPort = ssc.getLocalAddress().toString();
        this.ssc = ssc;
        this.ncFactory = ncFactory;
        this.acceptStrategy = acceptStrategy;
    }

    @Override
    public void eventLoop(final EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        if (!ssc.isOpen() || isClosed() || eventLoop.isClosing())
            throw new InvalidEventHandlerException();

        try {
            LOGGER.debug("accepting {}", ssc);

            final ChronicleSocketChannel sc = acceptStrategy.accept(ssc);

            if (sc != null) {
                if (isClosed() || eventLoop.isClosing()) {
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
            closeSocket();
            if (isClosed())
                throw new InvalidEventHandlerException();
            else
                throw new InvalidEventHandlerException(e);
        } catch (
                Exception e) {

            if (!isClosed() && !eventLoop.isClosing()) {
                final ChronicleServerSocket socket = ssc.socket();
                LOGGER.warn("{}, port={}", hostPort, socket == null ? "unknown" : socket.getLocalPort(), e);
            }
            closeSocket();
            throw new InvalidEventHandlerException(e);
        }
        return false;
    }

    private void closeSocket() {
        ssc.socket().close();
        ssc.close();
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
