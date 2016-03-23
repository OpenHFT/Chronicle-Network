package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.WireTcpHandler;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface ConnectionStrategy {
    boolean notifyConnected(@NotNull WireTcpHandler nc,
                            int localIdentifier,
                            int remoteIdentifier);
}
