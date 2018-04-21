package net.openhft.chronicle.network.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class handles the bridge between two sets of buffers:
 * - socket-side data - encrypted, handled by <code>readData</code>, <code>writeData</code>
 * - application-side data - decrypted, handled by <code>handleDecryptedData</code>
 */
public interface BufferHandler {
    /**
     * Read encrypted data from an input into the supplied buffer.
     *
     * @param target the target buffer for encrypted data
     * @return the number of bytes written
     * @throws IOException if an exception occurs
     */
    int readData(final ByteBuffer target) throws IOException;

    /**
     * Accept decrypted input data, previous collected by an invocation of <code>readData</code>.
     * Any plain-text output should be written to the output buffer for encryption.
     *
     * @param input  a buffer containing decrypted input data
     * @param output a buffer that can be used for writing plain-text output
     */
    void handleDecryptedData(final ByteBuffer input, final ByteBuffer output);

    /**
     * Write encrypted data to an output from the supplied buffer.
     *
     * @param encrypted the buffer containing encrypted data
     * @return the number of bytes written
     * @throws IOException if an exception occurs
     */
    int writeData(final ByteBuffer encrypted) throws IOException;
}
