package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.Closeable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ChronicleServerSocketFactory {

    public static ChronicleServerSocketChannel open() throws IOException {
        ServerSocketChannel ssc = ServerSocketChannel.open();

        return new ChronicleServerSocketChannel() {

            @Override
            public boolean isClosed() {
                throw new UnsupportedOperationException("todo");
            }

            @Override
            public ChronicleSocketChannel accept() throws IOException {
                ssc.configureBlocking(true);
                SocketChannel accept = ssc.accept();
                return ChronicleSocketChannelFactory.wrap(accept);
            }

            @Override
            public boolean isOpen() {
                return ssc.isOpen();
            }

            @Override
            public ChronicleServerSocket socket() {
                return new ChronicleServerSocket() {

                    @Override
                    public int getLocalPort() {
                        return ssc.socket().getLocalPort();
                    }

                    @Override
                    public void close() {
                        Closeable.closeQuietly(ssc.socket());
                    }

                    @Override
                    public void setReuseAddress(final boolean b) throws SocketException {
                        ssc.socket().setReuseAddress(b);
                    }

                    @Override
                    public SocketAddress getLocalSocketAddress() {
                        return ssc.socket().getLocalSocketAddress();
                    }
                };
            }

            @Override
            public void close() {
                Closeable.closeQuietly(ssc);
            }

            @Override
            public void bind(final InetSocketAddress address) throws IOException {
                ssc.bind(address);
            }

            @Override
            public SocketAddress getLocalAddress() throws IOException {
                return ssc.getLocalAddress();
            }

            @Override
            public void setOption(final SocketOption<Boolean> soReuseaddr, final boolean b) throws IOException {
                ssc.setOption(soReuseaddr, b);
            }

            @Override
            public void configureBlocking(final boolean configureBlocking) throws IOException {
                ssc.configureBlocking(configureBlocking);
            }
        };

    }
}
