/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.tcp.ChronicleServerSocket;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public class ClusterAcceptorEventHandler<C extends ClusterContext<C, T>, T extends ClusteredNetworkContext<T>> extends AbstractCloseable implements EventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAcceptorEventHandler.class);
    @NotNull
    private final ChronicleServerSocketChannel ssc;
    @NotNull
    private final C context;
    private final String hostPort;
    private EventLoop eventLoop;

    public ClusterAcceptorEventHandler(@NotNull final String hostPort,
                                       @NotNull final C context) throws IOException {
        this.hostPort = hostPort;
        this.ssc = TCPRegistry.acquireServerSocketChannel(this.hostPort);
        this.context = context;
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

            final ChronicleSocketChannel sc = ssc.accept();

            if (sc != null) {
                if (isClosed() || eventLoop.isClosing()) {
                    closeQuietly(sc);
                    throw new InvalidEventHandlerException("closed");
                }
                final T nc = context.networkContextFactory().apply(context);
                nc.socketChannel(sc);
                nc.isAcceptor(true);
                if (context.networkStatsListenerFactory() != null) {
                    final NetworkStatsListener<T> nsl = context.networkStatsListenerFactory().apply(context);
                    nc.networkStatsListener(nsl);
                    nsl.networkContext(nc);
                }
                final NetworkStatsListener<T> nl = nc.networkStatsListener();
                notifyHostPort(sc, nl);
                final TcpEventHandler<T> tcpEventHandler = context.tcpEventHandlerFactory().apply(nc);

                if (isClosed())
                    closeQuietly(tcpEventHandler);
                else
                    eventLoop.addHandler(tcpEventHandler);
            }
        } catch (AsynchronousCloseException e) {
            closeSocket();
            throw new InvalidEventHandlerException(e);
        } catch (ClosedChannelException e) {
            closeSocket();
            if (isClosed())
                throw InvalidEventHandlerException.reusable();
            else
                throw new InvalidEventHandlerException(e);
        } catch (Exception e) {
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
        closeQuietly(ssc);
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
