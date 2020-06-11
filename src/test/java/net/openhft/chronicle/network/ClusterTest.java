package net.openhft.chronicle.network;

import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClusterTest extends NetworkTestCommon {

    @Test
    public void testDeepCopy() {
        MyClusterContext cc = (MyClusterContext) new MyClusterContext().value(22).wireType(WireType.TEXT);
        String s = Marshallable.$toString(cc);
        MyClusterContext o = Marshallable.fromString(s);
        Assert.assertEquals(cc.value, o.value);
        MyClusterContext cc2 = cc.deepCopy();
        Assert.assertEquals(cc.value, cc2.value);

        try (Cluster<HostDetails, ?, MyClusterContext<?>> c = new MyCluster("mine")) {
            c.clusterContext(cc);
            try (Cluster<HostDetails, ?, MyClusterContext<?>> c2 = c.deepCopy()) {
                MyClusterContext mcc = c2.clusterContext();
                Assert.assertNotNull(mcc);
                Assert.assertEquals(22, mcc.value);
            }
        }
    }

    private static class MyCluster<T extends ClusteredNetworkContext<T>> extends Cluster<HostDetails, T, MyClusterContext<T>> {
        public MyCluster() {
            this("dummy");
        }

        MyCluster(String clusterName) {
            super(clusterName);
        }

        @Override
        @NotNull
        protected HostDetails newHostDetails() {
            return new HostDetails();
        }
    }

    static class MyClusterContext<T extends ClusteredNetworkContext<T>> extends net.openhft.chronicle.network.cluster.ClusterContext<T> {
        int value;

        @NotNull
        @Override
        public ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory() {
            return null;
        }

        public ClusterContext<T> value(int v) {
            this.value = v;
            return this;
        }

        @Override
        protected void defaults() {

        }
    }
}
