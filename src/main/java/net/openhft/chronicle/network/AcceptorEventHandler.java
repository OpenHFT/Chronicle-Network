/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.network.event.EventHandler;
import net.openhft.chronicle.network.event.EventLoop;
import net.openhft.chronicle.network.event.HandlerPriority;
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
public class AcceptorEventHandler implements EventHandler,Closeable {
    private final Supplier<TcpHandler> tcpHandlerSupplier;
    private EventLoop eventLoop;
    private final ServerSocketChannel ssc;

    private static final Logger LOG = LoggerFactory.getLogger(AcceptorEventHandler.class);

    public AcceptorEventHandler(int port, Supplier<TcpHandler> tcpHandlerSupplier) throws IOException {
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        ssc = ServerSocketChannel.open();
        ssc.socket().setReuseAddress(true);
        ssc.bind(new InetSocketAddress(port));
    }

    public int getLocalPort() throws IOException {
        return ssc.socket().getLocalPort();
    }

    @Override
    public void eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean runOnce()  {
        try {

            SocketChannel sc = ssc.accept();

            if (sc != null)
                eventLoop.addHandler(new TcpEventHandler(sc, tcpHandlerSupplier.get()));
        } catch (AsynchronousCloseException e) {
            closeSocket();
        } catch (Exception e) {
            LOG.error("", e);
            closeSocket();
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

    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    public boolean isDead() {
        return !ssc.isOpen();
    }

    @Override
    public void close() throws IOException {
        closeSocket();
    }
}
