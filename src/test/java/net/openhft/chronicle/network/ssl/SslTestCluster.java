package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

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
            connectionManager.addListener(new ConnectionManager.ConnectionListener() {
                @Override
                public void onConnectionChange(final NetworkContext nc, final boolean isConnected) {
                    if (nc.isAcceptor() || !isConnected || nc.isClosed()) {
                        return;
                    }

                    nc.wireOutPublisher().publish(PingTcpHandler.newPingHandler(clusterName(), nc.newCid()));
                }
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

    private static final class PingTcpHandler extends AbstractSubHandler<SslTestClusteredNetworkContext> implements Marshallable {
        @NotNull
        public static WriteMarshallable newPingHandler(final String csp, final long cid) {
            @NotNull final PingTcpHandler handler = new PingTcpHandler();

            return w -> w.writeDocument(true, d -> d.writeEventName(CoreFields.csp).text(csp)
                    .writeEventName(CoreFields.cid).int64(cid)
                    .writeEventName(CoreFields.handler).typedMarshallable(handler));
        }

        @Override
        public void readMarshallable(@NotNull final WireIn wire) throws IORuntimeException {
        }

        @Override
        public void onRead(@NotNull final WireIn inWire, @NotNull final WireOut outWire) {
            final StringBuilder eventName = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = inWire.readEventName(eventName);
            if ("ping".contentEquals(eventName)) {
                final long id = valueIn.int64();

                nc().wireOutPublisher().put(null, wireOut -> {
                    wireOut.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
                    wireOut.writeDocument(false,
                            d -> d.writeEventName("pong").int64(id));
                });
            } else
                if ("pong".contentEquals(eventName)) {
                    final long id = valueIn.int64();
                }
        }

        @Override
        public void onInitialize(final WireOut outWire) throws RejectedExecutionException {
            if (nc().isAcceptor()) {
                @NotNull WriteMarshallable writeMarshallable = newPingHandler(csp(), cid());
                publish(writeMarshallable);
                nc().eventLoop().addHandler(true, new PingSender(this::nc, this::localIdentifier, this::remoteIdentifier, cid()));
            }
        }
    }

    private static final class PingSender implements EventHandler {

        private final Supplier<NetworkContext> nc;
        private final IntSupplier local;
        private final IntSupplier remote;
        private final long cid;
        private long lastPublish = 0;
        private int counter;

        PingSender(final Supplier<NetworkContext> nc, final IntSupplier local, final IntSupplier remote, final long cid) {
            this.nc = nc;
            this.local = local;
            this.remote = remote;
            this.cid = cid;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            if (lastPublish < System.currentTimeMillis() - 5000L) {

                nc.get().wireOutPublisher().put(null, wireOut -> {
                    wireOut.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid));
                    wireOut.writeDocument(false,
                            d -> d.writeEventName("ping").int32(counter++));

                });
                lastPublish = System.currentTimeMillis();
            }
            return false;
        }
    }
}