package net.openhft.chronicle.network;

import java.util.UUID;

/**
 * used to collected stats about the network activity
 */
public interface NetworkStats<T extends NetworkStats> {

    T userId(String userId);

    long writeBps();

    T writeBps(long writeBps);

    long readBps();

    T readBps(long readBps);

    long socketPollCountPerSecond();

    T socketPollCountPerSecond(long socketPollCountPerSecond);

    long timestamp();

    T timestamp(long timestamp);

    void host(String hostName);

    void port(int port);

    T hostName(String hostName);

    String userId();

    long localIdentifier();

    T localIdentifier(long localIdentifer);

    long remoteIdentifier();

    T remoteIdentifier(long remoteIdentifier);

    void clientId(UUID clientId);

    UUID clientId();

    String hostName();

    int port();
}