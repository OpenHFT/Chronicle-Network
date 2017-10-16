package net.openhft.chronicle.network;

import java.nio.ByteBuffer;

public interface BufferModel {
    ByteBuffer getBufferForReading();
    void postRead(final ByteBuffer applicationInputData, final int networkDataReceived);
    ByteBuffer getBufferForWriting();
    void preWrite(final ByteBuffer applicationWrittenData);
}