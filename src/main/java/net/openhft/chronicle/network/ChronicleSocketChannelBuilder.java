package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.NetworkUtil.TCP_BUFFER_SIZE;

public class ChronicleSocketChannelBuilder {

    @NotNull
    private final InetSocketAddress socketAddress;
    private int tcpBufferSize = Jvm.getInteger("tcp.client.buffer.size", TCP_BUFFER_SIZE);
    private int socketConnectionTimeoutMs = Jvm.getInteger("client.timeout", 500);
    @Nullable
    private InetSocketAddress localBinding;
    private boolean tcpNoDelay = !TcpEventHandler.DISABLE_TCP_NODELAY;

    public ChronicleSocketChannelBuilder(@NotNull InetSocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    /**
     * Set the TCP buffer size
     *
     * @param tcpBufferSize The TCP buffer size in bytes
     * @return this
     */
    public ChronicleSocketChannelBuilder tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    /**
     * Set the socket connection timeout
     *
     * @param socketConnectionTimeoutMs the socket connection timeout in milliseconds
     * @return this
     */
    public ChronicleSocketChannelBuilder socketConnectionTimeoutMs(int socketConnectionTimeoutMs) {
        this.socketConnectionTimeoutMs = socketConnectionTimeoutMs;
        return this;
    }

    /**
     * Set whether to enable/disable <a href="https://en.wikipedia.org/wiki/Nagle%27s_algorithm">Nagle's algorithm</a> for this socket
     *
     * @param tcpNoDelay
     * @return
     */
    public ChronicleSocketChannelBuilder tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * Set the local socket to bind to
     *
     * @param localBinding The local socket to bind to, or null to not bind to any local socket
     * @return this
     */
    public ChronicleSocketChannelBuilder localBinding(InetSocketAddress localBinding) {
        this.localBinding = localBinding;
        return this;
    }

    /**
     * Open a socket channel with the builder's current settings
     *
     * @return The opened channel, or null if no channel could be opened
     * @throws IOException if something goes wrong opening the channel
     */
    @Nullable
    public ChronicleSocketChannel open() throws IOException {
        final ChronicleSocketChannel result = ChronicleSocketChannelFactory.wrap();
        @Nullable Selector selector = null;
        boolean failed = true;
        try {
            if (localBinding != null) {
                result.bind(localBinding);
            }
            result.configureBlocking(false);
            ChronicleSocket socket = result.socket();
            socket.setTcpNoDelay(tcpNoDelay);
            socket.setReceiveBufferSize(tcpBufferSize);
            socket.setSendBufferSize(tcpBufferSize);
            socket.setSoTimeout(0);
            socket.setSoLinger(false, 0);
            result.connect(socketAddress);

            selector = Selector.open();
            result.register(selector, SelectionKey.OP_CONNECT);

            int select = selector.select(socketConnectionTimeoutMs);
            if (select == 0) {
                if (Jvm.isDebugEnabled(ConnectionStrategy.class))
                    Jvm.debug().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
                return null;
            } else {
                try {
                    if (!result.finishConnect())
                        return null;

                } catch (IOException e) {
                    if (Jvm.isDebugEnabled(ConnectionStrategy.class))
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
