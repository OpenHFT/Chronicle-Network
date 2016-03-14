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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class RemoteConnector implements Closeable {
    public static final int BUFFER_SIZE = 8 << 20;

    private static final Logger LOG = LoggerFactory.getLogger(RemoteConnector.class);
    @NotNull
    private final Function<NetworkContext, TcpEventHandler> tcpHandlerSupplier;

    @NotNull
    // private final BiFunction<Boolean, SocketChannel, TcpEventHandler>  sessionDetailsSupplier;

    private final Integer tcpBufferSize;
    private volatile boolean closed;

    private volatile List<Closeable> closeables = new ArrayList<>();

    public RemoteConnector(@NotNull final Function<NetworkContext, TcpEventHandler>
                                   tcpHandlerSupplier) {
        this.tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", BUFFER_SIZE);
        this.tcpHandlerSupplier = tcpHandlerSupplier;
    }

    public void connect(final String remoteHostPort,
                        final EventLoop eventLoop,
                        NetworkContext nc,
                        final long timeOutMs) {

        final InetSocketAddress address = TCPRegistry.lookup(remoteHostPort);

        long timeoutTime = System.currentTimeMillis() + timeOutMs;
        final AtomicLong nextPeriod = new AtomicLong();
        eventLoop.addHandler(new RCEventHandler(nextPeriod, timeoutTime, remoteHostPort, nc,
                eventLoop, address));
    }


    private static void closeSocket(SocketChannel socketChannel) {
        if (socketChannel == null)
            return;
        try {
            final Socket socket = socketChannel.socket();
            if (socket != null)
                socket.close();
        } catch (IOException ignored) {
        }

        try {
            socketChannel.close();
        } catch (IOException ignored) {
        }
    }


    @Override
    public void close() {
        if (closed)
            return;

        closed = true;

        final List<Closeable> closeables = this.closeables;
        this.closeables = null;
        closeables.forEach(Closeable::closeQuietly);
    }


    private SocketChannel openSocketChannel(InetSocketAddress socketAddress) throws IOException {
        final SocketChannel result = SocketChannel.open(socketAddress);
        result.configureBlocking(false);
        Socket socket = result.socket();
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        socket.setSoTimeout(0);
        socket.setSoLinger(false, 0);


        return result;
    }

    class RCEventHandler implements EventHandler, Closeable {

        private final InetSocketAddress address;
        private volatile boolean closed;
        private final AtomicLong nextPeriod;
        private final long timeoutTime;
        private final String remoteHostPort;
        private final NetworkContext nc;
        private final EventLoop eventLoop;

        public RCEventHandler(AtomicLong nextPeriod, long timeoutTime, String remoteHostPort,
                              NetworkContext nc, EventLoop eventLoop, InetSocketAddress address) {
            this.nextPeriod = nextPeriod;
            this.timeoutTime = timeoutTime;
            this.remoteHostPort = remoteHostPort;
            this.nc = nc;
            this.eventLoop = eventLoop;
            this.address = address;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            if (closed)
                throw new InvalidEventHandlerException();
            final long time = System.currentTimeMillis();
            if (time > nextPeriod.get())
                nextPeriod.set(time + 1000);
            else
                return false;


            if (time > timeoutTime)
                throw Jvm.rethrow(new TimeoutException("timed out attempting to connect to " +
                        remoteHostPort + " as " + address));

            SocketChannel sc;

            try {
                sc = RemoteConnector.this.openSocketChannel(address);
            } catch (IOException e) {
                //    LOG.error("", e);
                return false;
            }

            if (sc == null)
                return false;

            if (LOG.isInfoEnabled())
                LOG.info("accepted connection " + sc);

            nc.socketChannel(sc);
            nc.isAcceptor(false);

            final TcpEventHandler eventHandler = tcpHandlerSupplier.apply(nc);
            eventLoop.addHandler(eventHandler);
            final List<Closeable> closeables = RemoteConnector.this.closeables;
            if (closeables == null)
                // we have died.
                Closeable.closeQuietly(eventHandler);
            else
                closeables.add(() -> closeSocket(sc));

            throw new InvalidEventHandlerException();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{"
                    + "remoteHostPort=" + remoteHostPort + "}";
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void notifyClosing() {
            closed = true;
        }
    }
}