package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.io.Closeable;
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
        return wrap(SocketChannel.open());
    }

    public static ChronicleSocketChannel wrap(InetSocketAddress socketAddress) throws IOException {
        return wrap(SocketChannel.open(socketAddress));
    }

    public static ChronicleSocketChannel wrap(@NotNull final SocketChannel sc) {
        return new ChronicleSocketChannelImpl(sc);
    }


}
