package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.cluster.HostDetails;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SslTestCluster extends Cluster<HostDetails, SslTestClusteredNetworkContext, SslTestClusterContext> {

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

            final ConnectionManager<SslTestClusteredNetworkContext> connectionManager = findConnectionManager(hostDetail.hostId());
            connectionManager.addListener((nc, isConnected) -> {
                if (nc.isAcceptor() || !isConnected || nc.isClosed()) {
                    return;
                }

                nc.wireOutPublisher().publish(PingTcpHandler.newPingHandler(clusterName(), nc.newCid()));
            });
        }
    }

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