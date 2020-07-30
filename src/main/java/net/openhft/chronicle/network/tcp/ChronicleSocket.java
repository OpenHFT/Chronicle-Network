package net.openhft.chronicle.network.tcp;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketOptions;

public interface ChronicleSocket {

    void setTcpNoDelay(final boolean b) throws SocketException;

    int getReceiveBufferSize() throws SocketException;

    void setReceiveBufferSize(final int tcpBuffer) throws SocketException;

    int getSendBufferSize() throws SocketException;

    void setSendBufferSize(final int tcpBuffer) throws SocketException;

    void setSoTimeout(int i) throws SocketException;

    /**
     * Enable/disable {@link SocketOptions#SO_LINGER SO_LINGER} with the specified linger time in seconds. The maximum timeout value is platform
     * specific.
     *
     * The setting only affects socket close.
     *
     * @param on     whether or not to linger on.
     * @param linger how long to linger for, if on is true.
     * @throws SocketException          if there is an error in the underlying protocol, such as a TCP error.
     * @throws IllegalArgumentException if the linger value is negative.
     * @since JDK1.1
     */
    void setSoLinger(boolean on, int linger) throws SocketException;

    void shutdownInput() throws IOException;

    void shutdownOutput() throws IOException;

    Object getRemoteSocketAddress();

    int getLocalPort();

}
