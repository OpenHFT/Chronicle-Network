package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class ChronicleSocketChannelImpl extends AbstractCloseable implements ChronicleSocketChannel {

    private final SocketChannel sc;

    ChronicleSocketChannelImpl(SocketChannel sc) {
        this.sc = sc;
    }

    @Override
    public void performClose() {
        Closeable.closeQuietly(sc);
    }

    @Override
    public @NotNull ChronicleSocketChannel socketChannel() {
        return ChronicleSocketChannelFactory.wrap(sc);
    }

    @Override
    public int read(final ByteBuffer byteBuffer) throws IOException {
        return sc.read(byteBuffer);
    }

    @Override
    public int write(final ByteBuffer byteBuffer) throws IOException {
        return sc.write(byteBuffer);
    }

    @Override
    public long write(final ByteBuffer[] byteBuffers) throws IOException {
        return sc.write(byteBuffers);
    }

    @Override
    public void configureBlocking(final boolean blocking) throws IOException {
        SocketChannel sc1 = sc;
        if (sc1 != null)
            sc1.configureBlocking(blocking);
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() throws IOException {
        return (InetSocketAddress) sc.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() throws IOException {
        return (InetSocketAddress) sc.getRemoteAddress();
    }

    @Override
    public boolean isOpen() {
        return sc.isOpen();
    }

    @Override
    public boolean isBlocking() {
        return sc.isBlocking();
    }

    @Override
    public ChronicleSocket socket() {
        SocketChannel sc1 = sc;
        if (sc1 == null)
            return null;
        return ChronicleSocketFactory.toChronicleSocket(sc1.socket());
    }

    @Override
    public void connect(final InetSocketAddress socketAddress) throws IOException {
        sc.connect(socketAddress);
    }

    @Override
    public void register(final Selector selector, final int opConnect) throws ClosedChannelException {
        sc.register(selector, opConnect);
    }

    @Override
    public boolean finishConnect() throws IOException {
        return sc.finishConnect();
    }

    @Override
    public void setOption(final SocketOption<Boolean> soReuseaddr, final boolean b) throws IOException {
        sc.setOption(soReuseaddr, b);
    }

    @Override
    public ISocketChannel toISocketChannel() {
        return ISocketChannel.wrap(socketChannel());
    }

};
