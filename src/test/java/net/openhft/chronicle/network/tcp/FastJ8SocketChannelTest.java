package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.network.NetworkTestCommon;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.jupiter.api.Assertions.assertThrows;

class FastJ8SocketChannelTest extends NetworkTestCommon {

    @Test
    void closedChannelExceptionIsThrownWhenAttemptIsMadeToReadFromClosedChannel() throws IOException {
        try (final ServerSocketChannel local = ServerSocketChannel.open().bind(new InetSocketAddress("localhost", 0))) {
            local.configureBlocking(false);
            try (final SocketChannel remote = SocketChannel.open(local.socket().getLocalSocketAddress())) {
                remote.configureBlocking(false);
                try (final FastJ8SocketChannel fastJ8SocketChannel = new FastJ8SocketChannel(remote)) {
                    remote.close();
                    assertThrows(ClosedChannelException.class, () -> fastJ8SocketChannel.read(ByteBuffer.allocateDirect(100)));
                }
            }
        }
    }

    @Test
    void closedChannelExceptionIsThrownWhenAttemptIsMadeToWriteToClosedChannel() throws IOException {
        try (final ServerSocketChannel local = ServerSocketChannel.open().bind(new InetSocketAddress("localhost", 0))) {
            local.configureBlocking(false);
            try (final SocketChannel remote = SocketChannel.open(local.socket().getLocalSocketAddress())) {
                remote.configureBlocking(false);
                try (final FastJ8SocketChannel fastJ8SocketChannel = new FastJ8SocketChannel(remote)) {
                    remote.close();
                    assertThrows(ClosedChannelException.class, () -> fastJ8SocketChannel.write(ByteBuffer.allocateDirect(100)));
                }
            }
        }
    }
}