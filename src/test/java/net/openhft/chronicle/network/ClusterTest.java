package net.openhft.chronicle.network;

import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClusterTest {

    @Test
    public void testDeepCopy() {
        Cluster c = new MyCluster("mine");
        c.clusterContext(new MyClusterContext(null));
        Cluster c2 = c.deepCopy();
        Assert.assertNotNull(c2.clusterContext());
        System.out.println(Marshallable.$toString(c2));
    }

    private static class MyCluster extends Cluster {
        public MyCluster() {
            this("dummy");
        }
        MyCluster(String clusterName) {
            super(clusterName);
        }

        @Override
        protected HostDetails newHostDetails() {
            return new HostDetails();
        }
    }

    private static class MyClusterContext extends net.openhft.chronicle.network.cluster.ClusterContext {
        public MyClusterContext(WireIn wireIn) {
        }

        @Override
        public ThrowingFunction<NetworkContext, TcpEventHandler, IOException> tcpEventHandlerFactory() {
            return null;
        }
    }
}
