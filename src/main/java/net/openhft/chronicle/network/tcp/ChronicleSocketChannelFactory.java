package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.tcp.ISocketChannel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public enum ChronicleSocketChannelFactory {
    ;

    public static ChronicleSocketChannel wrap() throws IOException {
        return newInstance(SocketChannel.open());
    }

    public static ChronicleSocketChannel wrap(InetSocketAddress socketAddress) throws IOException {
        return newInstance(SocketChannel.open(socketAddress));
    }

    public static ChronicleSocketChannel wrap(@NotNull final SocketChannel sc) {
        return newInstance(sc);
    }

    @NotNull
    private static ChronicleSocketChannel newInstance(@NotNull final SocketChannel sc) {
        return new ChronicleSocketChannel() {
            @Override
            public boolean isClosed() {
                throw new UnsupportedOperationException("todo");
            }

            @Override
            public void close() {
                Closeable.closeQuietly(sc);
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
                return ISocketChannel.wrap(sc);
            }

        };
    }

}
