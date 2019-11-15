package net.openhft.chronicle.network;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireParser;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClusterTest {

    @Test
    public void testDeepCopy() {
        MyClusterContext cc = (MyClusterContext) new MyClusterContext().value(22).wireType(WireType.TEXT);
        String s = Marshallable.$toString(cc);
        MyClusterContext o = Marshallable.fromString(s);
        Assert.assertEquals(cc.value, o.value);
        MyClusterContext cc2 = cc.deepCopy();
        Assert.assertEquals(cc.value, cc2.value);

        Cluster c = new MyCluster("mine");
        c.clusterContext(cc);
        Cluster c2 = c.deepCopy();
        MyClusterContext mcc = (MyClusterContext) c2.clusterContext();
        Assert.assertNotNull(mcc);
        Assert.assertEquals(22, mcc.value);
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
        int value;

        @UsedViaReflection
        protected MyClusterContext(WireIn wire) throws IORuntimeException {
            readMarshallable(wire);
        }

        public MyClusterContext() {
        }

        @Override
        protected WireParser wireParser() {
            return super.wireParser().register(() -> "value", (s, v) -> this.value = v.int32());
        }

        @Override
        public ThrowingFunction<NetworkContext, TcpEventHandler, IOException> tcpEventHandlerFactory() {
            return null;
        }

        public ClusterContext value(int v) {
            this.value = v;
            return this;
        }
    }
}
