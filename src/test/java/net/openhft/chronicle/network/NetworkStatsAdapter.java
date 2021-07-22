package net.openhft.chronicle.network;

/**
 * Adapter to make it easier to make inline NetworkStatsListeners
 *
 * @param <N> the NetworkContext type
 */
public class NetworkStatsAdapter<N extends NetworkContext<N>> implements NetworkStatsListener<N> {

    @Override
    public void networkContext(N networkContext) {
    }

    @Override
    public void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond) {
    }

    @Override
    public void onHostPort(String hostName, int port) {
    }

    @Override
    public void onRoundTripLatency(long nanosecondLatency) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
