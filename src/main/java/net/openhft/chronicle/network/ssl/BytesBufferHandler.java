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
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.internal.HeapBytesStore;
import net.openhft.chronicle.bytes.internal.NativeBytesStore;
import net.openhft.chronicle.core.internal.util.DirectBufferUtil;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
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
public final class BytesBufferHandler<N extends NetworkContext<N>> extends SimpleCloseable implements BufferHandler {
    private static final Bytes<ByteBuffer> EMPTY_APPLICATION_INPUT = Bytes.wrapForRead(ByteBuffer.allocate(0));
    private TcpHandler<N> delegateHandler;
    private Bytes<ByteBuffer> input;
    private Bytes<ByteBuffer> output;
    private N networkContext;

    private Reference<ByteBuffer> decryptedInBuf;
    private Bytes<ByteBuffer> decryptedInput;
    private Reference<ByteBuffer> decryptedOutBuf;
    private Bytes<ByteBuffer> decryptedOutput;

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
        input.read(target);
        return toRead;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDecryptedData(final ByteBuffer input, final ByteBuffer output) {
        boolean atStart = input.position() == 0;
        if (!atStart) {
            input.flip();
        }

        prepareBuffers(input, output);

        Bytes<ByteBuffer> applicationInput = decryptedInput;
        if (atStart) {
            applicationInput = EMPTY_APPLICATION_INPUT;
        }

        delegateHandler.process(applicationInput, decryptedOutput, networkContext);
        output.position((int) decryptedOutput.writePosition());

        input.position((int) decryptedInput.readPosition());
        if (decryptedInput.readPosition() != 0) {
            input.compact();
        }
    }

    private void prepareBuffers(ByteBuffer input, ByteBuffer output) {
        if ((decryptedInBuf == null || input != decryptedInBuf.get()) || decryptedInput == null) {
            if (decryptedInput != null) decryptedInput.releaseLast();

            decryptedInput = wrap(input);
            decryptedInBuf = new WeakReference<>(input);
        }
        decryptedInput.readPosition(input.position());
        decryptedInput.readLimit(input.limit());

        if ((decryptedOutBuf == null || output != decryptedOutBuf.get()) || decryptedOutput == null) {
            if (decryptedOutput != null) decryptedOutput.releaseLast();

            decryptedOutput = wrap(output);
            decryptedOutBuf = new WeakReference<>(output);
        }
        decryptedOutput.writePosition(output.position());
        decryptedOutput.writeLimit(output.limit());
    }

    /**
     * Modified {@link BytesStore#wrap(ByteBuffer)} that does not take ownership of bb and does not release it.
     */
    private Bytes<ByteBuffer> wrap(ByteBuffer bb) {
        BytesStore<?, ByteBuffer> bs = bb.isDirect()
                ? new NativeBytesStore<>(DirectBufferUtil.addressOrThrow(bb), bb.capacity())
                : HeapBytesStore.wrap(bb);

        try {
            return bs.bytesForRead();
        } finally {
            bs.release(ReferenceOwner.INIT);
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
        output.writeSome(encrypted);
        return toWrite;
    }

    @Override
    protected void performClose() {
        if (decryptedOutput != null)
            decryptedOutput.releaseLast();

        if (decryptedInput != null)
            decryptedInput.releaseLast();
    }
}