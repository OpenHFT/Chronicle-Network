package net.openhft.chronicle.network;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface NetworkStatsListener<N extends NetworkContext> {
    void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond,
                        @NotNull N networkContext);

    void onHostPort(String hostName, int port);
}
