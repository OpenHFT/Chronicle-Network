package net.openhft.chronicle.network.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

abstract class AbstractSocketBufferHandler implements BufferHandler {
    private final SocketChannel channel;

    AbstractSocketBufferHandler(final SocketChannel socketChannel) {
        this.channel = socketChannel;
    }

    @Override
    public int readData(final ByteBuffer target) throws IOException {
        return channel.read(target);
    }

    @Override
    public int writeData(final ByteBuffer encrypted) throws IOException {
        return channel.write(encrypted);
    }
}
