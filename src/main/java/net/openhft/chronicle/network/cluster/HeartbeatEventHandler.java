package net.openhft.chronicle.network.cluster;

/**
 * @author Rob Austin.
 */
public interface HeartbeatEventHandler {
    void onMessageReceived();
}
