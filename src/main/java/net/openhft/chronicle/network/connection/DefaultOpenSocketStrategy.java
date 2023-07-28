package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class DefaultOpenSocketStrategy implements OpenSocketStrategy {

    public static DefaultOpenSocketStrategy INSTANCE = new DefaultOpenSocketStrategy();

    private DefaultOpenSocketStrategy() {
    }

    /**
     * the reason for this method is that unlike the selector it uses tick time
     */
    @Nullable
    public ChronicleSocketChannel openSocketChannel(ConnectionStrategy connectionStrategy,
                                                    @NotNull InetSocketAddress socketAddress,
                                                    int tcpBufferSize,
                                                    long timeoutMs,
                                                    int socketConnectionTimeoutMs) throws IOException, InterruptedException {
        assert timeoutMs > 0;
        long start = System.currentTimeMillis();
        ChronicleSocketChannel sc = ChronicleSocketChannel.builder(socketAddress)
                .tcpBufferSize(tcpBufferSize)
                .socketConnectionTimeoutMs(socketConnectionTimeoutMs)
                .localBinding(connectionStrategy.localSocketBinding())
                .open();
        if (sc != null)
            return sc;

        for (; ; ) {
            connectionStrategy.throwExceptionIfClosed();
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            long startMs = System.currentTimeMillis();
            if (start + timeoutMs < startMs) {
                Jvm.startup().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
                return null;
            }
            sc = ChronicleSocketChannel.builder(socketAddress)
                    .tcpBufferSize(tcpBufferSize)
                    .socketConnectionTimeoutMs(socketConnectionTimeoutMs)
                    .localBinding(connectionStrategy.localSocketBinding())
                    .open();
            if (sc != null)
                return sc;
            Thread.yield();
            // If nothing is listening, socketChannel returns pretty much immediately so we support a pause here
            pauseBeforeReconnect(connectionStrategy, startMs);
        }
    }

    void pauseBeforeReconnect(ConnectionStrategy connectionStrategy, long startMs) {
        long pauseMillis = (startMs + connectionStrategy.pauseMillisBeforeReconnect()) - System.currentTimeMillis();
        if (Jvm.isDebugEnabled(this.getClass()))
            Jvm.debug().on(this.getClass(), "Waiting for reconnect " + pauseMillis + " ms");
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(pauseMillis));
    }
}
