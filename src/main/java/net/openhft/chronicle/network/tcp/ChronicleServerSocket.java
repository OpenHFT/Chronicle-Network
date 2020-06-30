package net.openhft.chronicle.network.tcp;

import java.net.SocketAddress;
import java.net.SocketException;

public interface ChronicleServerSocket {
    int getLocalPort();

    void close();

    void setReuseAddress(boolean b) throws SocketException;

    SocketAddress getLocalSocketAddress();
}
