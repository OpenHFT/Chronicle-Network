package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class AbstractSocketBufferHandler implements BufferHandler {
    private final ChronicleSocketChannel channel;

    AbstractSocketBufferHandler(final ChronicleSocketChannel socketChannel) {
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
