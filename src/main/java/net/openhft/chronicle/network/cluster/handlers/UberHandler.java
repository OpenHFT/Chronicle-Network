package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.api.session.WritableSubHandler;
import net.openhft.chronicle.network.cluster.*;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.network.HeaderTcpHandler.HANDLER;
import static net.openhft.chronicle.network.cluster.TerminatorHandler.terminationHandler;

public final class UberHandler<T extends ClusteredNetworkContext<T>> extends CspTcpHandler<T> implements
        Demarshallable,
        WriteMarshallable {

    private final int remoteIdentifier;
    private final int localIdentifier;
    @NotNull
    private final AtomicBoolean isClosing = new AtomicBoolean();
    @Nullable
    private ConnectionChangedNotifier<T> connectionChangedNotifier;
    @NotNull
    private String clusterName;

    @UsedViaReflection
    private UberHandler(@NotNull final WireIn wire) {
        remoteIdentifier = wire.read(() -> "remoteIdentifier").int32();
        localIdentifier = wire.read(() -> "localIdentifier").int32();
        @Nullable final WireType wireType = wire.read(() -> "wireType").object(WireType.class);
        clusterName = wire.read(() -> "clusterName").text();
        wireType(wireType);
    }

    private UberHandler(final int localIdentifier,
                        final int remoteIdentifier,
                        @NotNull final WireType wireType,
                        @NotNull final String clusterName) {

        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;

        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;
        this.clusterName = clusterName;
        wireType(wireType);
    }

    @Override
    public String toString() {
        return "UberHandler{" +
                "clusterName='" + clusterName + '\'' +
                ", remoteIdentifier=" + remoteIdentifier +
                ", localIdentifier=" + localIdentifier +
                ", isClosing=" + isClosing +
                '}';
    }

    private static WriteMarshallable uberHandler(final WriteMarshallable m) {
        return wire -> {
            try (final DocumentContext ignored = wire.writingDocument(true)) {
                wire.write(() -> HANDLER).typedMarshallable(m);
            }
        };
    }

    private static String peekContents(@NotNull final DocumentContext dc) {
        try {
            return dc.wire().readingPeekYaml();
        } catch (RuntimeException e) {
            return "Failed to peek at contents due to: " + e.getMessage();
        }
    }

    public int remoteIdentifier() {
        return remoteIdentifier;
    }

    @Override
    public boolean isClosed() {
        return isClosing.get();
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wire) {
        wire.write(() -> "remoteIdentifier").int32(localIdentifier);
        wire.write(() -> "localIdentifier").int32(remoteIdentifier);
        final WireType value = wireType();
        wire.write(() -> "wireType").object(value);
        wire.write(() -> "clusterName").text(clusterName);
    }

    @Override
    protected void onInitialize() {
        final ClusteredNetworkContext<T> nc = nc();
        nc.wireType(wireType());
        isAcceptor(nc.isAcceptor());

        assert checkIdentifierEqualsHostId();
        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;

        final WireOutPublisher publisher = nc.wireOutPublisher();
        publisher(publisher);

        if (!nc.isValidCluster(clusterName)) {
            Jvm.warn().on(getClass(), "cluster=" + clusterName, new RuntimeException("cluster not " +
                    "found, cluster=" + clusterName));
            return;
        }

        @NotNull final EventLoop eventLoop = nc.eventLoop();
        if (!eventLoop.isClosed()) {
            eventLoop.start();

            final Cluster<?, T, ? extends ClusterContext<T>> engineCluster = nc.getCluster(clusterName);

            // note : we have to publish the uber handler, even if we send a termination event
            // this is so the termination event can be processed by the receiver
            if (nc().isAcceptor()) {
                // reflect the uber handler
                publish(uberHandler());
            }

            nc.terminationEventHandler(engineCluster.findTerminationEventHandler(remoteIdentifier));

            if (!checkConnectionStrategy(engineCluster)) {
                // the strategy has told us to reject this connection, we have to first notify the
                // other host, we will do this by sending a termination event
                publish(terminationHandler(localIdentifier, remoteIdentifier, nc.newCid()));
                closeSoon();
                return;
            }

            if (!isClosing.get())
                notifyConnectionListeners(engineCluster);
        }
    }

    private boolean checkIdentifierEqualsHostId() {
        return localIdentifier == nc().getLocalHostIdentifier() || 0 == nc().getLocalHostIdentifier();
    }

    private void notifyConnectionListeners(@NotNull final Cluster cluster) {
        connectionChangedNotifier = cluster.findClusterNotifier(remoteIdentifier);
        if (connectionChangedNotifier != null)
            connectionChangedNotifier.onConnectionChanged(true, nc());
    }

    private boolean checkConnectionStrategy(@NotNull final Cluster cluster) {
        final ConnectionStrategy strategy = cluster.findConnectionStrategy(remoteIdentifier);
        return strategy == null ||
                strategy.notifyConnected(this, localIdentifier, remoteIdentifier);
    }

    private WriteMarshallable uberHandler() {
        @NotNull final UberHandler handler = new UberHandler(
                localIdentifier,
                remoteIdentifier,
                wireType(),
                clusterName);

        return uberHandler(handler);
    }

    /**
     * wait 2 seconds before closing the socket connection, this should allow time of the
     * termination event to be sent.
     */
    private void closeSoon() {
        isClosing.set(true);
        @NotNull final ScheduledExecutorService closer = newSingleThreadScheduledExecutor(new NamedThreadFactory("closer", true));
        closer.schedule(() -> {
            closer.shutdown();
            close();
        }, 2, SECONDS);
    }

    @Override
    public void close() {
        if (!isClosing.getAndSet(true) && connectionChangedNotifier != null)
            connectionChangedNotifier.onConnectionChanged(false, nc());

        try {
            nc().acquireConnectionListener().onDisconnected(localIdentifier, remoteIdentifier(), nc().isAcceptor());
        } catch (Exception e) {
            Jvm.fatal().on(this.getClass(), e);
        }
        super.close();
    }

    @Override
    protected void onRead(@NotNull final DocumentContext dc, @NotNull final WireOut outWire) {
        try {
            if (isClosing.get())
                return;

            onMessageReceivedOrWritten();

            final Wire inWire = dc.wire();
            if (dc.isMetaData()) {
                if (!readMeta(inWire))
                    return;

                final SubHandler<T> handler = handler();
                handler.remoteIdentifier(remoteIdentifier);
                handler.localIdentifier(localIdentifier);
                try {
                    handler.onInitialize(outWire);
                } catch (RejectedExecutionException e) {
                    Jvm.warn().on(getClass(), "EventGroup shutdown", e);
                    removeHandler(handler);
                } catch (RejectedHandlerException ex) {
                    Jvm.debug().on(getClass(), "Removing rejected handler: " + handler);
                    removeHandler(handler);
                }
                return;
            }

            final SubHandler<T> handler = handler();
            if (handler != null && dc.isData() && !inWire.bytes().isEmpty())
                try {
                    handler.onRead(inWire, outWire);
                } catch (RejectedHandlerException ex) {
                    Jvm.debug().on(getClass(), "Removing rejected handler: " + handler);
                    removeHandler(handler);
                }
            else
                Jvm.warn().on(getClass(), "handler == null, check that the " +
                        "Csp/Cid has been sent, failed to " +
                        "fully " +
                        "process the following " +
                        "YAML\n");
        } catch (Throwable e) {
            Jvm.warn().on(getClass(), "failed to parse:" + peekContents(dc), e);
        }
    }

    @Override
    protected void onBytesWritten() {
        onMessageReceivedOrWritten();
    }

    /**
     * ready to accept wire
     *
     * @param outWire the wire that you wish to write
     */
    @Override
    protected void onWrite(@NotNull final WireOut outWire) {
        for (int i = 0; i < writers.size(); i++)
            try {
                if (isClosing.get())
                    return;
                final WritableSubHandler<T> w = writers.get(i);
                if (w != null)
                    w.onWrite(outWire);
            } catch (Exception e) {
                Jvm.fatal().on(getClass(), e);
            }
    }

    private void onMessageReceivedOrWritten() {
        final HeartbeatEventHandler heartbeatEventHandler = heartbeatEventHandler();
        if (heartbeatEventHandler != null)
            heartbeatEventHandler.onMessageReceived();
    }

    public static final class Factory<T extends ClusteredNetworkContext<T>> implements
            BiFunction<ClusterContext<T>,
            HostDetails,
            WriteMarshallable>,
            Demarshallable {

        @UsedViaReflection
        private Factory(@NotNull WireIn wireIn) {}

        public Factory() {}

        @NotNull
        public WriteMarshallable apply(@NotNull final ClusterContext<T> clusterContext,
                                       @NotNull final HostDetails hostdetails) {
            final byte localIdentifier = clusterContext.localIdentifier();
            final int remoteIdentifier = hostdetails.hostId();
            final WireType wireType = clusterContext.wireType();
            final String name = clusterContext.clusterName();
            return uberHandler(new UberHandler<>(localIdentifier,
                    remoteIdentifier,
                    wireType,
                    name));
        }
    }
}

