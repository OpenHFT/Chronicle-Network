package net.openhft.chronicle.network.tcp;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.UnsupportedAddressTypeException;

public interface ChronicleServerSocketChannel extends Closeable {

    ChronicleSocketChannel accept() throws IOException;

    boolean isOpen();

    ChronicleServerSocket socket();

    void close();

    /**
     * Binds the channel's socket to a local address and configures the socket
     * to listen for connections.
     *
     * <p> An invocation of this method is equivalent to the following:
     * <blockquote><pre>
     * bind(local, 0);
     * </pre></blockquote>
     *
     * @param address The local address to bind the socket, or {@code null} to bind
     *                to an automatically assigned socket address
     * @throws AlreadyBoundException           if bound
     * @throws UnsupportedAddressTypeException if address unsupported
     * @throws ClosedChannelException          if channel is closed
     * @throws IOException                     if unable to open
     * @throws SecurityException               If a security manager has been installed and its {@link SecurityManager#checkListen checkListen}
     *                                         method denies the operation
     * @since 1.7
     */
    void bind(InetSocketAddress address) throws IOException;

    SocketAddress getLocalAddress() throws IOException;

    void setOption(SocketOption<Boolean> soReuseaddr, boolean b) throws IOException;

    void configureBlocking(boolean b) throws IOException;

    String hostPort();
}
