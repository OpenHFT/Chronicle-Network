package net.openhft.chronicle.network.tcp;

import java.io.IOException;
import java.net.SocketException;

public interface ChronicleSocket {
    void setTcpNoDelay(final boolean b) throws SocketException;

    int getReceiveBufferSize() throws SocketException;

    void setReceiveBufferSize(final int tcpBuffer) throws SocketException;

    int getSendBufferSize() throws SocketException;

    void setSendBufferSize(final int tcpBuffer) throws SocketException;

    void setSoTimeout(int i) throws SocketException;

    void setSoLinger(boolean b, int i) throws SocketException;

    void shutdownInput() throws IOException;

    void shutdownOutput() throws IOException;

    Object getRemoteSocketAddress();

    int getLocalPort();

}
