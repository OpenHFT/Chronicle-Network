package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;

import java.nio.ByteBuffer;

/**
 * This class handles the bridge between two sets of buffers:
 * - socket-side data - encrypted, handled by <code>readData</code>, <code>writeData</code>
 * - application-side data - decrypted, handled by <code>handleDecryptedData</code>
 * <p>
 * The delegate {@see TcpHandler} will be invoked with decrypted input data, and its
 * output will be encrypted read for transmission over a socket.
 *
 * @param <N> the type of {@see NetworkContext}
 */
public final class BytesBufferHandler<N extends NetworkContext> implements BufferHandler {
    private static final Bytes<ByteBuffer> EMPTY_APPLICATION_INPUT = Bytes.wrapForRead(ByteBuffer.allocate(0));
    private TcpHandler<N> delegateHandler;
    private Bytes<ByteBuffer> input;
    private Bytes<ByteBuffer> output;
    private N networkContext;

    public void set(
            final TcpHandler<N> delegate,
            final Bytes<ByteBuffer> input,
            final Bytes<ByteBuffer> output,
            final N networkContext) {
        this.delegateHandler = delegate;
        this.input = input;
        this.output = output;
        this.networkContext = networkContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int readData(final ByteBuffer target) {
        final int toRead = Math.min(target.remaining(), (int) input.readRemaining());
        for (int i = 0; i < toRead; i++) {
            target.put(input.readByte());
        }
        return toRead;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDecryptedData(final ByteBuffer input, final ByteBuffer output) {
        final Bytes<ByteBuffer> applicationInput;
        if (input.position() != 0) {
            input.flip();
            applicationInput = Bytes.wrapForRead(input);
        } else {
            applicationInput = EMPTY_APPLICATION_INPUT;
        }

        final Bytes<ByteBuffer> applicationOutput = Bytes.wrapForWrite(output);
        delegateHandler.process(applicationInput, applicationOutput, networkContext);
        output.position((int) applicationOutput.writePosition());

        input.position((int) applicationInput.readPosition());
        if (applicationInput.readPosition() != 0) {
            input.compact();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int writeData(final ByteBuffer encrypted) {
        if (output.readPosition() != 0) {
            output.compact();
        }
        final int writeRemaining = (int)
                (output.writeRemaining() > Integer.MAX_VALUE ?
                        Integer.MAX_VALUE : output.writeRemaining());
        final int toWrite = Math.min(encrypted.remaining(), writeRemaining);
        for (int i = 0; i < toWrite; i++) {
            output.writeByte(encrypted.get());
        }
        return toWrite;
    }
}