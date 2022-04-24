package net.openhft.chronicle.network.test;

import net.openhft.chronicle.network.cluster.Cluster;

/**
 * A very minimal {@link Cluster} implementation for use in tests
 */
public class TestCluster extends Cluster<TestClusteredNetworkContext, TestClusterContext> {
    public TestCluster(TestClusterContext clusterContext) {
        super();
        clusterContext(clusterContext);
        clusterContext.cluster(this);
    }
}
