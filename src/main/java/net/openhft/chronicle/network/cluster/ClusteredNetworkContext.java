package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkContext;

public interface ClusteredNetworkContext<T extends NetworkContext> extends NetworkContext<T> {
    default EventLoop eventLoop() {
        throw new UnsupportedOperationException();
    }

    byte getLocalHostIdentifier();

    boolean isValidCluster(final String clusterName);

    Cluster getCluster(final String clusterName);
}