package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.jetbrains.annotations.NotNull;

public class VanillaClusteredNetworkContext<T extends VanillaClusteredNetworkContext<T, C>, C extends ClusterContext<C, T>>
        extends VanillaNetworkContext<T> implements ClusteredNetworkContext<T> {

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
        Jvm.warn().on(VanillaClusteredNetworkContext.class, "Missed heartbeat on network context " + socketChannel());
        return false;
    }
}
