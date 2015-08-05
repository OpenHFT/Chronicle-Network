package net.openhft.chronicle.network;

/**
 * @author Rob Austin.
 */

public interface ClientClosedProvider {
    /**
     * @return {@true} if the client has intentionally closed
     */
    default boolean hasClientClosed() {
        return false;
    }
}
