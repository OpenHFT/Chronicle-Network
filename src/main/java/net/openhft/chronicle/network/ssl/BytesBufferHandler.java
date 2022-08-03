/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;

import java.nio.ByteBuffer;

/**
 * This class handles the bridge between two sets of buffers:
 * - socket-side data - encrypted, handled by <code>readData</code>, <code>writeData</code>
 * - application-side data - decrypted, handled by <code>handleDecryptedData</code>
 * <p>
 * The delegate {@link TcpHandler} will be invoked with decrypted input data, and its
 * output will be encrypted read for transmission over a socket.
 *
 * @param <N> the type of {@link NetworkContext}
 */
public final class BytesBufferHandler<N extends NetworkContext<N>> implements BufferHandler {
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
            IOTools.unmonitor(applicationInput); // temporary wrapper
        } else {
            applicationInput = EMPTY_APPLICATION_INPUT;
        }

        final Bytes<ByteBuffer> applicationOutput = Bytes.wrapForWrite(output);
        try {
            delegateHandler.process(applicationInput, applicationOutput, networkContext);
            output.position((int) applicationOutput.writePosition());
        } finally {
            applicationOutput.releaseLast();
        }

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