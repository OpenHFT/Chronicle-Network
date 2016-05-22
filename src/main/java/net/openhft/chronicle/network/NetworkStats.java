package net.openhft.chronicle.network;

/**
 * used to collected stats about the network activity
 */
public interface NetworkStats  {

    long writeBps();

    NetworkStats writeBps(long writeBps);

    long readBps();

    NetworkStats readBps(long readBps);

    long socketPollCountPerSecond();

    NetworkStats socketPollCountPerSecond(long socketPollCountPerSecond);

    long timestamp();

    NetworkStats timestamp(long timestamp);

    void host(String hostName);

    void port(int port);
}