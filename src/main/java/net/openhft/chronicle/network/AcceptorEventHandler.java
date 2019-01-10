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
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

/*
 * Created by peter.lawrey on 22/01/15.
 */
public class AcceptorEventHandler implements EventHandler, Closeable {
    @NotNull
    private final Function<NetworkContext, TcpEventHandler> handlerFactory;
    @NotNull

    private final ServerSocketChannel ssc;
    @NotNull
    private final Supplier<? extends NetworkContext> ncFactory;
    private final String hostPort;
    private final AcceptStrategy acceptStrategy;

    private EventLoop eventLoop;

    private volatile boolean closed;

    public AcceptorEventHandler(@NotNull final String hostPort,
                                @NotNull final Function<NetworkContext, TcpEventHandler> handlerFactory,
                                @NotNull final Supplier<? extends NetworkContext> ncFactory) throws IOException {
        this(hostPort, handlerFactory, ncFactory, AcceptStrategy.ACCEPT_ALL);
    }

    public AcceptorEventHandler(@NotNull final String hostPort,
                                @NotNull final Function<NetworkContext, TcpEventHandler> handlerFactory,
                                @NotNull final Supplier<? extends NetworkContext> ncFactory,
                                @NotNull final AcceptStrategy acceptStrategy) throws IOException {
        this.handlerFactory = handlerFactory;
        this.hostPort = hostPort;
        this.ssc = TCPRegistry.acquireServerSocketChannel(this.hostPort);
        this.ncFactory = ncFactory;
        this.acceptStrategy = acceptStrategy;
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
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), Thread.currentThread() + " accepting " + ssc);

            SocketChannel sc = acceptStrategy.accept(ssc);

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

        } catch (ClosedByInterruptException e) {
            closeSocket();
            throw new InvalidEventHandlerException(e);
        } catch (Exception e) {

            if (!closed) {
                ServerSocket socket = ssc.socket();
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
    public void close() {
        closed = true;
        closeSocket();
    }

}
