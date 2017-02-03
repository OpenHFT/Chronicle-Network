package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rob Austin.
 */
public interface FatalFailureMonitor {
    Logger LOG = LoggerFactory.getLogger(FatalFailureMonitor.class);

    /**
     * called if all the connection attempts/(and/or timeouts) determined by the connection strategy has been exhausted
     *
     * @param name    the name of the connection
     * @param message reason
     */
    default void onFatalFailure(@Nullable String name, String message) {
        LOG.error("name=" + name + ",message=" + message);
    }
}
