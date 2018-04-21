package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.threads.Timer;
import net.openhft.chronicle.core.threads.VanillaEventHandler;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class HeartbeatHandler<T extends ClusteredNetworkContext> extends AbstractSubHandler<T> implements
        Demarshallable, WriteMarshallable, HeartbeatEventHandler {

    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final AtomicBoolean hasHeartbeats = new AtomicBoolean();
    private volatile long lastTimeMessageReceived;
    @Nullable
    private ConnectionListener connectionMonitor;
    @Nullable
    private Timer timer;

    @UsedViaReflection
    public HeartbeatHandler(@NotNull WireIn w) {
        heartbeatTimeoutMs = w.read(() -> "heartbeatTimeoutMs").int64();
        heartbeatIntervalMs = w.read(() -> "heartbeatIntervalMs").int64();
        assert heartbeatTimeoutMs >= 1000 :
                "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", this is too small";
        assert heartbeatIntervalMs >= 500 :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", this is too small";
        onMessageReceived();

    }

    private HeartbeatHandler(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        assert heartbeatTimeoutMs > heartbeatIntervalMs :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", " +
                        "heartbeatTimeoutMs=" + heartbeatTimeoutMs;

        assert heartbeatTimeoutMs >= 1000 :
                "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", this is too small";
        assert heartbeatIntervalMs >= 500 :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", this is too small";
    }

    private static WriteMarshallable heartbeatHandler(final long heartbeatTimeoutMs,
                                                      final long heartbeatIntervalMs,
                                                      final long cid) {
        return w -> w.writeDocument(true,
                d -> d.writeEventName(CoreFields.csp).text("/")
                        .writeEventName(CoreFields.cid).int64(cid)
                        .writeEventName(CoreFields.handler).typedMarshallable(new
                                HeartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs)));
    }

    @Override
    public void onInitialize(@NotNull WireOut outWire) {

        if (nc().isAcceptor())
            heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs, cid()).writeMarshallable
                    (outWire);

        @NotNull final WriteMarshallable heartbeatMessage = w -> {
            w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
            w.writeDocument(false, d -> d.write(() -> "heartbeat").text(""));
        };

        connectionMonitor = nc().acquireConnectionListener();
        timer = new Timer(nc().eventLoop());
        startPeriodicHeartbeatCheck();
        startPeriodicallySendingHeartbeats(heartbeatMessage);
    }

    private void startPeriodicallySendingHeartbeats(WriteMarshallable heartbeatMessage) {

        @NotNull final VanillaEventHandler task = () -> {
            if (isClosed())
                throw new InvalidEventHandlerException("closed");
            // we will only publish a heartbeat if the wire out publisher is empty
            WireOutPublisher wireOutPublisher = nc().wireOutPublisher();
            if (wireOutPublisher.isEmpty())
                wireOutPublisher.publish(heartbeatMessage);
            return true;
        };

        timer.scheduleAtFixedRate(task, this.heartbeatIntervalMs, this.heartbeatIntervalMs);
    }

    @Override
    public boolean isClosed() {
        return closable().isClosed();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut w) {
        w.write(() -> "heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        assert heartbeatIntervalMs > 0;
        w.write(() -> "heartbeatIntervalMs").int64(heartbeatIntervalMs);
    }

    @Override
    public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
        if (inWire.isEmpty())
            return;
        inWire.read(() -> "heartbeat").text();
    }

    @Override
    public void close() {
        if (connectionMonitor != null)
            connectionMonitor.onDisconnected(localIdentifier(), remoteIdentifier(), nc().isAcceptor());
        if (closable().isClosed())
            return;
        lastTimeMessageReceived = Long.MAX_VALUE;
        Closeable.closeQuietly(closable());
    }

    @Override
    public void onMessageReceived() {
        lastTimeMessageReceived = System.currentTimeMillis();
    }

    private VanillaEventHandler heartbeatCheck() {

        return () -> {

            if (this.closable().isClosed())
                throw new InvalidEventHandlerException("closed");

            boolean hasHeartbeats = hasReceivedHeartbeat();
            boolean prev = this.hasHeartbeats.getAndSet(hasHeartbeats);

            if (hasHeartbeats != prev) {
                if (!hasHeartbeats) {
                    connectionMonitor.onDisconnected(this.localIdentifier(),
                            this.remoteIdentifier(), nc().isAcceptor());

                    this.close();

                    final Runnable socketReconnector = nc().socketReconnector();

                    // if we have been terminated then we should not attempt to reconnect
                    if (nc().terminationEventHandler().isTerminated() && socketReconnector != null)
                        socketReconnector.run();

                    throw new InvalidEventHandlerException("closed");
                } else
                    connectionMonitor.onConnected(this.localIdentifier(),
                            this.remoteIdentifier(), nc().isAcceptor());
            }

            return true;
        };
    }

    /**
     * periodically check that messages have been received, ie heartbeats
     */
    private void startPeriodicHeartbeatCheck() {
        timer.scheduleAtFixedRate(heartbeatCheck(), 0, heartbeatTimeoutMs);
    }

    /**
     * called periodically to check that the heartbeat has been received
     *
     * @return {@code true} if we have received a heartbeat recently
     */
    private boolean hasReceivedHeartbeat() {
        long currentTimeMillis = System.currentTimeMillis();
        boolean result = lastTimeMessageReceived + heartbeatTimeoutMs >= currentTimeMillis;

        if (!result)
            Jvm.warn().on(getClass(), Integer.toHexString(hashCode()) + " missed heartbeat, lastTimeMessageReceived=" + lastTimeMessageReceived
                    + ", currentTimeMillis=" + currentTimeMillis);
        return result;
    }

    public static class Factory implements Function<ClusterContext, WriteMarshallable>,
            Demarshallable {

        @UsedViaReflection
        private Factory(WireIn w) {
        }

        public Factory() {
        }

        @NotNull
        @Override
        public WriteMarshallable apply(@NotNull ClusterContext clusterContext) {
            long heartbeatTimeoutMs = clusterContext.heartbeatTimeoutMs();
            long heartbeatIntervalMs = clusterContext.heartbeatIntervalMs();
            return heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs,
                    HeartbeatHandler.class.hashCode());
        }
    }
}