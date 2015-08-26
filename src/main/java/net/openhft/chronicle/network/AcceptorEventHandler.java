/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class AcceptorEventHandler implements EventHandler, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(AcceptorEventHandler.class);
    @NotNull
    private final Supplier<TcpHandler> tcpHandlerSupplier;
    @NotNull
    private final Supplier<SessionDetailsProvider> sessionDetailsSupplier;
    private final ServerSocketChannel ssc;
    private final long heartbeatIntervalTicks;
    private final long heartbeatTimeOutTicks;
    private EventLoop eventLoop;
    private boolean unchecked = false;
    private volatile boolean closed;

    public AcceptorEventHandler(@NotNull String description,
                                @NotNull final Supplier<TcpHandler> tcpHandlerSupplier,
                                @NotNull final Supplier<SessionDetailsProvider> sessionDetailsSupplier,
                                long heartbeatIntervalTicks, long heartbeatTimeOutTicks) throws
            IOException {
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        ssc = TCPRegistry.acquireServerSocketChannel(description);
        this.sessionDetailsSupplier = sessionDetailsSupplier;
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        this.heartbeatTimeOutTicks = heartbeatTimeOutTicks;
    }

    public void unchecked(boolean unchecked) {
        this.unchecked = unchecked;
    }

    @Override
    public void eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        if (!ssc.isOpen())
            throw new InvalidEventHandlerException();

        try {
            SocketChannel sc = ssc.accept();

            if (sc != null) {
                if (LOG.isInfoEnabled())
                    LOG.info("Accepted " + sc);

                final SessionDetailsProvider sessionDetails = sessionDetailsSupplier.get();

                sessionDetails.setClientAddress((InetSocketAddress) sc.getRemoteAddress());

                eventLoop.addHandler(new TcpEventHandler(sc,
                        tcpHandlerSupplier.get(),
                        sessionDetails, unchecked,
                        heartbeatIntervalTicks, heartbeatTimeOutTicks));
            }

        } catch (AsynchronousCloseException e) {
            closeSocket();
        } catch (Exception e) {
            if (!closed) {
                LOG.error("", e);
                closeSocket();
            }
        }
        return false;
    }

    private void closeSocket() {
        try {
            ssc.socket().close();
        } catch (IOException ignored) {
        }

        try {
            ssc.close();
        } catch (IOException ignored) {
        }
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        closeSocket();
    }
}
