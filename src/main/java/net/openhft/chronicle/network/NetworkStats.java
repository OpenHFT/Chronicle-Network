package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * used to collected stats about the network activity
 */
public class NetworkStats extends AbstractMarshallable {

    int bytesWrittenCount, noByteWrittenCount, bytesReadCount;

    public int bytesWrittenCount() {
        return bytesWrittenCount;
    }

    public NetworkStats bytesWrittenCount(int bytesWrittenCount) {
        this.bytesWrittenCount = bytesWrittenCount;
        return this;
    }

    public int noByteWrittenCount() {
        return noByteWrittenCount;
    }

    public NetworkStats noByteWrittenCount(int noByteWrittenCount) {
        this.noByteWrittenCount = noByteWrittenCount;
        return this;
    }

    public int bytesReadCount() {
        return bytesReadCount;
    }

    public NetworkStats bytesReadCount(int bytesReadCount) {
        this.bytesReadCount = bytesReadCount;
        return this;
    }
}