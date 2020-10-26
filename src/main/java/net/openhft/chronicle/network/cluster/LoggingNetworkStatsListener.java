/*
 * Copyright (c) 2016-2020 chronicle.software
 */
package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;

@SuppressWarnings("rawtypes")
public enum LoggingNetworkStatsListener implements NetworkStatsListener {
    INSTANCE;

    @Override
    public void networkContext(final NetworkContext networkContext) {
        throwExceptionIfClosed();

    }

    @Override
    public void onNetworkStats(final long writeBps, final long readBps, final long socketPollCountPerSecond) {
        throwExceptionIfClosed();

        if (Jvm.isDebugEnabled(LoggingNetworkStatsListener.class))
            Jvm.debug().on(LoggingNetworkStatsListener.class, String.format(
                    "networkStats: writeBps %d, readBps %d, pollCount/sec %d",
                    writeBps, readBps, socketPollCountPerSecond));

    }

    @Override
    public void onHostPort(final String hostName, final int port) {
        throwExceptionIfClosed();

        if (Jvm.isDebugEnabled(LoggingNetworkStatsListener.class))
            Jvm.debug().on(LoggingNetworkStatsListener.class, String.format("onHostPort %s, %d",
                    hostName, port));
    }

    @Override
    public void onRoundTripLatency(final long latencyNanos) {
        throwExceptionIfClosed();

        if (Jvm.isDebugEnabled(LoggingNetworkStatsListener.class))
            Jvm.debug().on(LoggingNetworkStatsListener.class, String.format("onRoundTripLatency %d", latencyNanos));
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}