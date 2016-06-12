package net.openhft.chronicle.network;

/**
 * @author Rob Austin.
 */
public interface HeartbeatListener {

    /**
     * called when we don't receive a heartbeat ( or in some cases any message )
     */
    void onMissedHeartbeat();
}
