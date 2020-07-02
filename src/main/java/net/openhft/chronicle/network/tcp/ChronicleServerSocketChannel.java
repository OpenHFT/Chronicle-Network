package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.Closeable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;

public interface ChronicleServerSocketChannel extends Closeable {

    ChronicleSocketChannel accept() throws IOException;

    boolean isOpen();

    ChronicleServerSocket socket();

    void close();

    void bind(InetSocketAddress address) throws IOException;

    SocketAddress getLocalAddress() throws IOException;

    void setOption(SocketOption<Boolean> soReuseaddr, boolean b) throws IOException;

    void configureBlocking(boolean b) throws IOException;
}
