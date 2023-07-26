package net.openhft.chronicle.network.connection;

/**
 * The state a connection reached, used to provide information to a {@link net.openhft.chronicle.network.ConnectionStrategy}
 * to be used when deciding which endpoint to try next
 */
public enum ConnectionState {
    /**
     * The connection was established and e.g. logged in/handshaked successfully.
     */
    ESTABLISHED,
    /**
     * The connection opened but did not complete handshake etc.
     */
    FAILED,
    /**
     * There's no information about the state this connection reached
     */
    UNKNOWN
}
