package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * used to collected stats about the network activity
 */
public class NetworkStats extends AbstractMarshallable {

    long writeBps, readBps, socketPollCountPerSecond;

    // bytes per seconds
    public long writeBps() {
        return writeBps;
    }

    // bytes per seconds
    public NetworkStats writeBps(long writeBps) {
        this.writeBps = writeBps;
        return this;
    }

    // bytes per seconds
    public long readBps() {
        return readBps;
    }

    // bytes per seconds
    public NetworkStats readBps(long readBps) {
        this.readBps = readBps;
        return this;
    }

    public long socketPollCountPerSecond() {
        return socketPollCountPerSecond;
    }

    public NetworkStats socketPollCountPerSecond(long socketPollCountPerSecond) {
        this.socketPollCountPerSecond = socketPollCountPerSecond;
        return this;
    }
}