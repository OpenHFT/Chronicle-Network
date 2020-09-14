package net.openhft.chronicle.network;

import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClusterTest extends NetworkTestCommon {

    @Test
    public <T extends ClusteredNetworkContext<T>> void testDeepCopy() {
        MyClusterContext<T> cc = new MyClusterContext<T>().value(22).wireType(WireType.TEXT);
        String s = Marshallable.$toString(cc);
        MyClusterContext<T> o = Marshallable.fromString(s);
        Assert.assertEquals(cc.value, o.value);
        MyClusterContext<T> cc2 = cc.deepCopy();
        Assert.assertEquals(cc.value, cc2.value);

        try (Cluster<T, MyClusterContext<T>> c = new MyCluster()) {
            c.clusterContext(cc);
            try (Cluster<T, MyClusterContext<T>> c2 = c.deepCopy()) {
                MyClusterContext<T> mcc = c2.clusterContext();
                Assert.assertNotNull(mcc);
                Assert.assertEquals(22, mcc.value);
            }
        }
    }

    private static class MyCluster<T extends ClusteredNetworkContext<T>> extends Cluster<T, MyClusterContext<T>> {
        public MyCluster() {
            super();
        }
    }

    static class MyClusterContext<T extends ClusteredNetworkContext<T>> extends net.openhft.chronicle.network.cluster.ClusterContext<MyClusterContext<T>, T> {
        int value;

        @Override
        protected String clusterNamePrefix() {
            return "";
        }

        @NotNull
        @Override
        public ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory() {
            return null;
        }

        public MyClusterContext<T> value(int v) {
            this.value = v;
            return this;
        }

        @Override
        protected void defaults() {

        }
    }
}
