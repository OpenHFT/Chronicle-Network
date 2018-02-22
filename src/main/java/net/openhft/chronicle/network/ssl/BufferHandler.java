package net.openhft.chronicle.network.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BufferHandler {
    int readData(final ByteBuffer target) throws IOException;
    void handleDecryptedData(final ByteBuffer input, final ByteBuffer output);
    int writeData(final ByteBuffer encrypted) throws IOException;
}
