package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.WritableSubHandler;
import net.openhft.chronicle.network.cluster.*;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

public class UberHandlerTest extends NetworkTestCommon {

    public static final int SIZE_OF_BIG_PAYLOAD = 300 * 1024;

    @Before
    public void before() {
        YamlLogging.setAll(true);
        System.setProperty("TcpEventHandler.tcpBufferSize", "131072");
    }

    @After
    public void after() {
        YamlLogging.setAll(false);
        System.clearProperty("TcpEventHandler.tcpBufferSize");
    }

    @Test
    public <T extends ClusteredNetworkContext<T>> void test() throws IOException {
        String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        HostDetails initiatorHost = new HostDetails().hostId(2).connectUri(desc);
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri(desc);

        try (MyClusterContext acceptorCtx = clusterContext(acceptorHost, initiatorHost);
             MyClusterContext initiatorCtx = clusterContext(initiatorHost, acceptorHost)) {

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                System.out.println("connection changed, isConnected " + isConnected);
                nc.wireOutPublisher().publish(w ->
                        w.writeDocument(true, d -> sendPingPong(d, 12345)));
                nc.wireOutPublisher().publish(w ->
                        w.writeDocument(true, d -> sendPingPong(d, 12346)));
                nc.wireOutPublisher().publish(w ->
                        w.writeDocument(true, d -> sendPingPong(d, 12347)));
                nc.wireOutPublisher().publish(w ->
                        w.writeDocument(true, d -> sendPingPong(d, 12348)));
            });

            // TODO: set HB numbers relatively low
            // TODO: add handlers to deterministically pause
            // TODO: send in a large message (>128K) to make bytes resize happen

            Jvm.pause(10_000);
        }
    }

    private void sendPingPong(WireOut wireOut, int cid) {
        wireOut.writeEventName(CoreFields.csp).text("pingpong")
                .writeEventName(CoreFields.cid).int64(cid)
                .writeEventName(CoreFields.handler).typedMarshallable(new PingPongHandler(false));
    }

    @NotNull
    private MyClusterContext clusterContext(HostDetails... clusterHosts) {
        MyClusterContext ctx = new MyClusterContext().wireType(WireType.BINARY).localIdentifier((byte) clusterHosts[0].hostId());
        ctx.heartbeatIntervalMs(501);
        MyCluster cluster = new MyCluster(ctx);
        for (HostDetails details : clusterHosts) {
            cluster.hostDetails.put(String.valueOf(details.hostId()), details);
        }
        return ctx;
    }

    static class MyClusteredNetworkContext extends VanillaClusteredNetworkContext<MyClusteredNetworkContext, MyClusterContext> {
        public MyClusteredNetworkContext(@NotNull MyClusterContext clusterContext) {
            super(clusterContext);
        }
    }

    static class MyCluster extends Cluster<MyClusteredNetworkContext, MyClusterContext> {
        MyCluster(MyClusterContext clusterContext) {
            super();
            clusterContext(clusterContext);
            clusterContext.cluster(this);
        }
    }

    static class MyClusterContext extends net.openhft.chronicle.network.cluster.ClusterContext<MyClusterContext, MyClusteredNetworkContext> {
        @Override
        protected String clusterNamePrefix() {
            return "";
        }


        @NotNull
        @Override
        public ThrowingFunction<MyClusteredNetworkContext, TcpEventHandler<MyClusteredNetworkContext>, IOException> tcpEventHandlerFactory() {
            return nc -> {
                if (nc.isAcceptor()) {
                    nc.wireOutPublisher(new VanillaWireOutPublisher(wireType()));
                }
                final TcpEventHandler<MyClusteredNetworkContext> handler = new TcpEventHandler<>(nc);
                final Function<MyClusteredNetworkContext, TcpHandler<MyClusteredNetworkContext>> factory =
                        unused -> new HeaderTcpHandler<>(handler, o -> (TcpHandler<MyClusteredNetworkContext>) o);
                final WireTypeSniffingTcpHandler<MyClusteredNetworkContext> sniffer = new WireTypeSniffingTcpHandler<>(handler, factory);
                handler.tcpHandler(sniffer);
                return handler;
            };
        }

        @Override
        protected void defaults() {
            if (this.wireOutPublisherFactory() == null)
                this.wireOutPublisherFactory(VanillaWireOutPublisher::new);

            if (serverThreadingStrategy() == null)
                this.serverThreadingStrategy(ServerThreadingStrategy.SINGLE_THREADED);

            if (this.networkContextFactory() == null)
                this.networkContextFactory(MyClusteredNetworkContext::new);
        }
    }

    static class PingPongHandler extends AbstractSubHandler<MyClusteredNetworkContext> implements
            Marshallable, WritableSubHandler<MyClusteredNetworkContext> {

        private static final int MAX_ROUNDS = 100;

        private boolean initiator;
        private boolean initiated = false;
        private int round = 0;

        public PingPongHandler(boolean initiator) {
            this.initiator = initiator;
        }

        @Override
        public void onInitialize(WireOut outWire) throws RejectedExecutionException {
            // Send back the handler for the other side
            if (!initiator) {
                outWire.writeDocument(true, d ->
                        d.writeEventName(CoreFields.csp).text("pingpong")
                                .writeEventName(CoreFields.cid).int64(cid())
                                .writeEventName(CoreFields.handler).typedMarshallable(new PingPongHandler(!initiator))
                );
            }
        }

        @Override
        public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
            final StringBuilder eventName = Wires.acquireStringBuilder();
            final ValueIn valueIn = inWire.readEventName(eventName);

            sendPingPongCid(outWire);
            try (final DocumentContext dc = outWire.writingDocument(false)) {
                round++;
                if (initiator && round > MAX_ROUNDS) {
                    outWire.write("stop").text("now");
                    close();
                } else if ("ping".equals(eventName.toString())) {
                    System.out.println("Read " + valueIn.bytes().length);
                    writeRandomJunk("pong", dc, round);
                } else if ("pong".equals(eventName.toString())) {
                    System.out.println("Read " + valueIn.bytes().length);
                    writeRandomJunk("ping", dc, round);
                } else if ("stop".equals(eventName.toString())) {
                    System.out.println("Stopping");
                    close();
                } else {
                    throw new IllegalStateException("Got unknown event: " + eventName);
                }
            }
        }

        private void writeRandomJunk(String eventName, DocumentContext dc, int counter) {
            Bytes<?> bigJunk = Bytes.allocateElasticOnHeap();
            int payloadSize = (int) ((round / (float) MAX_ROUNDS) * SIZE_OF_BIG_PAYLOAD);
            for (int i = 0; i < payloadSize; i++) {
                bigJunk.writeByte((byte) i);
            }
            dc.wire().write(eventName).bytes(bigJunk)
                    .write("counter").int32(counter);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("initiator").bool(initiator);
        }

        @Override
        public void onWrite(WireOut outWire) {
            if (initiator && !initiated) {
                sendPingPongCid(outWire);
                try (final DocumentContext dc = outWire.writingDocument()) {
                    writeRandomJunk("ping", dc, round++);
                }
                initiated = true;
            }
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            initiator = wire.read("initiator").bool();
        }

        private void sendPingPongCid(WireOut outWire) {
            try (final DocumentContext dc = outWire.writingDocument(true)) {
                dc.wire().write(CoreFields.cid).int64(cid());
            }
        }
    }
}
