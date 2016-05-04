package net.openhft.chronicle.network;

/**
 * @author Rob Austin.
 */
public interface ConnectionListener {

    void onConnected(int localIdentifier, int remoteIdentifier);

    void onDisconnected(int localIdentifier, int remoteIdentifier);
}
