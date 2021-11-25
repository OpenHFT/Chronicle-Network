package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VanillaClusteredNetworkContext<T extends VanillaClusteredNetworkContext<T, C>, C extends ClusterContext<C, T>>
        extends VanillaNetworkContext<T> implements ClusteredNetworkContext<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaClusteredNetworkContext.class);

    @NotNull
    private final EventLoop eventLoop;

    @NotNull
    protected final C clusterContext;

    public VanillaClusteredNetworkContext(@NotNull C clusterContext) {
        this.clusterContext = clusterContext;
        this.eventLoop = clusterContext.eventLoop();
        heartbeatListener(this::logMissedHeartbeat);
        serverThreadingStrategy(clusterContext.serverThreadingStrategy());
    }

    @Override
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @Override
    public byte getLocalHostIdentifier() {
        return clusterContext.localIdentifier();
    }

    @Override
    public C clusterContext() {
        return clusterContext;
    }

    private boolean logMissedHeartbeat() {
        LOGGER.warn("Missed heartbeat on network context " + socketChannel());
        return false;
    }
}
