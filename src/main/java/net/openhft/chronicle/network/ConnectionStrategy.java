package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.connection.FatalFailureMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;


public interface ConnectionStrategy extends Marshallable {


    /**
     * @param name                  the name of the connection, only used for logging
     * @param socketAddressSupplier
     * @param networkStatsListener
     * @param didLogIn              was the last attempt successfull, was a login established
     * @param fatalFailureMonitor
     * @return
     * @throws InterruptedException
     */
    SocketChannel connect(@NotNull String name,
                          @NotNull SocketAddressSupplier socketAddressSupplier,
                          @Nullable NetworkStatsListener<? extends NetworkContext> networkStatsListener,
                          boolean didLogIn,
                          @Nullable FatalFailureMonitor fatalFailureMonitor) throws InterruptedException;

    @Nullable
    default SocketChannel openSocketChannel(@NotNull InetSocketAddress socketAddress,
                                            int tcpBufferSize,
                                            long timeoutMs) throws IOException, InterruptedException {

        long start = Time.tickTime();
        for (; ; ) {
            if (start + timeoutMs < Time.tickTime())
                return null;
            SocketChannel sc = socketChannel(socketAddress, tcpBufferSize);
            if (sc != null)
                return sc;

            for (int i = 0; i < 100; i++) {
                Thread.sleep(1);
                Time.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }

        }
    }

    @Nullable
    static SocketChannel socketChannel(@NotNull InetSocketAddress socketAddress, int tcpBufferSize) throws IOException {

        final SocketChannel result = SocketChannel.open();
        @Nullable Selector selector = null;
        boolean failed = true;
        try {
            result.configureBlocking(false);
            Socket socket = result.socket();
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(tcpBufferSize);
            socket.setSendBufferSize(tcpBufferSize);
            socket.setSoTimeout(0);
            socket.setSoLinger(false, 0);
            result.connect(socketAddress);

            selector = Selector.open();
            result.register(selector, SelectionKey.OP_CONNECT);

            int select = selector.select(2500);
            if (select == 0) {
                Jvm.warn().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
                return null;
            } else {
                try {
                    if (!result.finishConnect())
                        return null;

                } catch (IOException e) {
                    Jvm.debug().on(ConnectionStrategy.class, "Failed to connect to " + socketAddress + " " + e);
                    return null;
                }
            }

            failed = false;
            return result;

        } catch (Exception e) {
            return null;
        } finally {
            closeQuietly(selector);
            if (failed)
                closeQuietly(result);
        }
    }


}
