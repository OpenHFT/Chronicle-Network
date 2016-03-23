package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.NetworkContext;

/**
 * @author Rob Austin.
 */
interface ConnectionChangedNotifier {
    void onConnectionChanged(boolean isConnected, final NetworkContext nc);
}
