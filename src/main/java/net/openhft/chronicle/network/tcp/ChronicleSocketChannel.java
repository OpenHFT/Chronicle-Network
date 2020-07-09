package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.Closeable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface ChronicleSocketChannel extends Closeable {

    int read(ByteBuffer byteBuffer) throws IOException;

    int write(ByteBuffer byteBuffer) throws IOException;

    long write(ByteBuffer[] byteBuffers) throws IOException;

    void configureBlocking(boolean blocking) throws IOException;

    InetSocketAddress getLocalAddress() throws IOException;

    InetSocketAddress getRemoteAddress() throws IOException;

    boolean isOpen();

    boolean isBlocking();

    ChronicleSocket socket();

    void connect(InetSocketAddress socketAddress) throws IOException;

    void register(Selector selector, int opConnect) throws ClosedChannelException;

    boolean finishConnect() throws IOException;

    void setOption(SocketOption<Boolean> soReuseaddr, boolean b) throws IOException;

    SocketChannel socketChannel();
}
