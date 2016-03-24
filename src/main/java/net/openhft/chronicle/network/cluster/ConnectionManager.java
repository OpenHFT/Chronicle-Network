package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.NetworkContext;

/**
 * @author Rob Austin.
 */
public interface ConnectionManager extends ConnectionChangedNotifier {

    /**
     * notifies the strategy that a message has been received, ( any message is assumed to be a
     * heartbeat )
     */
    void addListener(ConnectionListener connectionListener);

    interface ConnectionListener {
        void onConnectionChange(NetworkContext nc, boolean isConnected);
    }


}
