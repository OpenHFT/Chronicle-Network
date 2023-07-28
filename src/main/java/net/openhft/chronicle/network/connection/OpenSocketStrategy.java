package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface OpenSocketStrategy {

    /**
     * the reason for this method is that unlike the selector it uses tick time
     */
    @Nullable
    ChronicleSocketChannel openSocketChannel(ConnectionStrategy connectionStrategy,
                                             @NotNull InetSocketAddress socketAddress,
                                             int tcpBufferSize,
                                             long timeoutMs,
                                             int socketConnectionTimeoutMs) throws IOException, InterruptedException;
}
