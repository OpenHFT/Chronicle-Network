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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class AcceptorEventHandler implements EventHandler, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(AcceptorEventHandler.class);
    @NotNull
    private final Function<NetworkContext, TcpEventHandler> handlerFactory;
    @NotNull

    private final ServerSocketChannel ssc;
    @NotNull
    private final Supplier<? extends NetworkContext> ncFactory;

    private EventLoop eventLoop;

    private volatile boolean closed;

    public AcceptorEventHandler(@NotNull String description,
                                @NotNull final Function<NetworkContext, TcpEventHandler> handlerFactory,
                                @NotNull Supplier<NetworkContext> ncFactory)
            throws IOException {
        this.handlerFactory = handlerFactory;
        this.ssc = TCPRegistry.acquireServerSocketChannel(description);
        this.ncFactory = ncFactory;
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
            if (LOG.isDebugEnabled())
                LOG.debug(Thread.currentThread() + " accepting " + ssc);

            SocketChannel sc = ssc.accept();

            if (sc != null) {
                final NetworkContext nc = ncFactory.get();
                nc.socketChannel(sc);
                nc.isAcceptor(true);
                NetworkStatsListener nl = nc.networkStatsListener();
                if (nl != null)
                    notifyHostPort(sc, nl);
                TcpEventHandler apply = handlerFactory.apply(nc);
                eventLoop.addHandler(apply);

            }

        } catch (AsynchronousCloseException e) {
            closeSocket();
        } catch (Exception e) {
            if (!closed) {
                Jvm.fatal().on(getClass(), e);
                closeSocket();
            }
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
    public void close() throws IOException {
        closed = true;
        closeSocket();
    }
}
