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

import net.openhft.chronicle.core.io.Closeable;
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
import java.util.function.Consumer;
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
    private InetSocketAddress address;
    private SocketChannel sc;
    private List<Closeable> closeables = new ArrayList<>();

    public RemoteConnector(@NotNull final Function<NetworkContext, TcpEventHandler>
                                   tcpHandlerSupplier) {
        this.tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", BUFFER_SIZE);
        this.tcpHandlerSupplier = tcpHandlerSupplier;
    }


    public void connect(final String remoteHostPort,
                        final EventLoop eventLoop,
                        final Consumer<SocketChannel> onHandshaking,
                        NetworkContext nc,
                        final double timeOutMs) throws
            InvalidEventHandlerException {

        this.address = TCPRegistry.lookup(remoteHostPort);


        try {

            final long start = System.currentTimeMillis();
            Exception exception = null;
            for (; ; ) {
                try {
                    sc = openSocketChannel(address);
                    exception = null;
                    break;
                } catch (IOException e) {
                    if (exception == null)
                        exception = e;
                    // sleep then retry
                    Thread.sleep(100);
                    if (System.currentTimeMillis() - timeOutMs > start)
                        throw exception;
                }
            }

            if (sc != null) {

                if (LOG.isInfoEnabled())
                    LOG.info("accepted connection " + sc);

                onHandshaking.accept(sc);
                sc.configureBlocking(false);

                nc.socketChannel(sc);
                nc.isServerSocket(false);

                final TcpEventHandler eventHandler = tcpHandlerSupplier.apply(nc);

                eventLoop.addHandler(eventHandler);
                closeables.add(eventHandler);
            }

        } catch (Exception e) {
            if (!closed) {
                LOG.error("", e);
                closeSocket();
            }
        }
    }

    private void closeSocket() {
        SocketChannel socketChannel = sc;
        if (socketChannel == null)
            return;
        try {
            final Socket socket = sc.socket();
            if (socket != null)
                socket.close();
        } catch (IOException ignored) {
        }

        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }


    @Override
    public void close() {
        if (closed)
            return;

        closed = true;
        closeSocket();
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

}