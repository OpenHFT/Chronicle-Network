package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;

public final class SslTestClusteredNetworkContext
        extends VanillaNetworkContext implements ClusteredNetworkContext {
    private final byte hostId;
    private final Cluster cluster;
    private final EventLoop eventLoop;

    SslTestClusteredNetworkContext(final byte hostId, final Cluster cluster, final EventLoop eventLoop) {
        this.hostId = hostId;
        this.cluster = cluster;
        this.eventLoop = eventLoop;
    }

    @Override
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @Override
    public byte getLocalHostIdentifier() {
        return hostId;
    }

    @Override
    public boolean isValidCluster(final String clusterName) {
        return true;
    }

    @Override
    public Cluster getCluster(final String clusterName) {
        return cluster;
    }
}
