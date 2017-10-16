package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class StandardBufferModel implements BufferModel {
    @NotNull
    private final Bytes<ByteBuffer> inBBB;
    @NotNull
    private final Bytes<ByteBuffer> outBBB;

    StandardBufferModel(@NotNull final Bytes<ByteBuffer> inBBB,
                        @NotNull final Bytes<ByteBuffer> outBBB) {
        this.inBBB = inBBB;
        this.outBBB = outBBB;
    }

    @Override
    public ByteBuffer getBufferForReading() {
        return inBBB.underlyingObject();
    }

    @Override
    public void postRead(final ByteBuffer applicationInputData, final int networkDataReceived) {
        // no-op
    }

    @Override
    public ByteBuffer getBufferForWriting() {
        return outBBB.underlyingObject();
    }

    @Override
    public void preWrite(final ByteBuffer applicationWrittenData) {
        // no-op
    }
}
