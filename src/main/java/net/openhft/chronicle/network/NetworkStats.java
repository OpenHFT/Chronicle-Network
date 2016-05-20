package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * used to collected stats about the network activity
 */
public class NetworkStats extends AbstractMarshallable {

    int bytesWrittenPerSecond, socketPollCountPerSecond, bytesReadPerSecond;

    public int bytesWrittenCount() {
        return bytesWrittenPerSecond;
    }

    public NetworkStats bytesWrittenCount(int bytesWrittenCount) {
        this.bytesWrittenPerSecond = bytesWrittenCount;
        return this;
    }

    public int socketPollCount() {
        return socketPollCountPerSecond;
    }

    public NetworkStats socketPollCount(int socketPollCount) {
        this.socketPollCountPerSecond = socketPollCount;
        return this;
    }

    public int bytesReadCount() {
        return bytesReadPerSecond;
    }

    public NetworkStats bytesReadCount(int bytesReadCount) {
        this.bytesReadPerSecond = bytesReadCount;
        return this;
    }
}