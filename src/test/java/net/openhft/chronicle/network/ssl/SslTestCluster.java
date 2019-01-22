package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.cluster.HostDetails;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SslTestCluster extends Cluster<HostDetails, SslTestClusterContext> {

    public SslTestCluster(final String clusterName) {
        super(clusterName);
    }

    @Override
    public void install() {
        super.install();
        for (HostDetails hostDetail : hostDetails()) {
            if (hostDetail.hostId() == clusterContext().localIdentifier()) {
                continue;
            }

            final ConnectionManager connectionManager = findConnectionManager(hostDetail.hostId());
            connectionManager.addListener((nc, isConnected) -> {
                if (nc.isAcceptor() || !isConnected || nc.isClosed()) {
                    return;
                }

                nc.wireOutPublisher().publish(PingTcpHandler.newPingHandler(clusterName(), nc.newCid()));
            });
        }
    }

    @Nullable
    @Override
    public SslTestClusterContext clusterContext() {
        return super.clusterContext();
    }

    @NotNull
    @Override
    protected HostDetails newHostDetails() {
        return new HostDetails();
    }

}