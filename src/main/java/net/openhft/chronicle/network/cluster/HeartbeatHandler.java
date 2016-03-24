package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * will periodically send a heatbeat message, the period of this message is defined by {@link
 * HeartbeatHandler#heartbeatIntervalTicks} once the heart beat is
 *
 * @author Rob Austin.
 */
public class HeartbeatHandler<T extends NetworkContext> extends AbstractSubHandler<T> implements
        Demarshallable, WriteMarshallable, HeartbeatEventHandler {

    public static class Factory implements Function<ClusterContext, WriteMarshallable>,
            Demarshallable {

        private Factory(WireIn w) {
        }

        @Override
        public WriteMarshallable apply(ClusterContext clusterContext) {
            long heartbeatTimeoutMs = clusterContext.heartbeatTimeoutMs();
            long heartbeatIntervalTicks = clusterContext.heartbeatIntervalTicks();
            return heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalTicks, 0);
        }
    }

    public static final ScheduledExecutorService HEARTBEAT_EXECUTOR =
            newSingleThreadScheduledExecutor(new
                    NamedThreadFactory("RemoteConnector"));

    private final long heartbeatIntervalTicks;
    private long lastTimeMessageReceived;
    private final long heartbeatTimeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean hasHeartbeat = new AtomicBoolean();

    @UsedViaReflection
    protected HeartbeatHandler(@NotNull WireIn w) {
        heartbeatTimeoutMs = w.read(() -> "heartbeatTimeoutMs").int64();
        heartbeatIntervalTicks = w.read(() -> "heartbeatIntervalTicks").int64();
        startHeartbeatCheck();
    }

    private HeartbeatHandler(long heartbeatTimeoutMs, long heartbeatIntervalTicks) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        assert heartbeatTimeoutMs > heartbeatIntervalTicks :
                "heartbeatIntervalTicks=" + heartbeatIntervalTicks + ", " +
                        "heartbeatTimeoutMs=" + heartbeatTimeoutMs;
    }

    @Override
    public void onInitialize(WireOut outWire) {

        if (nc().isAcceptor())
            heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalTicks, cid()).writeMarshallable
                    (outWire);

        final WriteMarshallable heartbeatMessage = w -> {
            w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
            w.writeDocument(false, d -> d.write(() -> "heartbeat").text(""));
        };

        final Runnable task = () -> {
            // we will only publish a heartbeat if the wire out publisher is empty
            if (nc().wireOutPublisher().isEmpty())
                return;
            nc().wireOutPublisher().put("", heartbeatMessage);
        };

        HEARTBEAT_EXECUTOR.schedule(task, this.heartbeatIntervalTicks, MILLISECONDS);
    }

    private static WriteMarshallable heartbeatHandler(final long heartbeatTimeoutMs,
                                                      final long heartbeatIntervalTicks,
                                                      final long cid) {
        return w -> w.writeDocument(true,
                d -> d.writeEventName(CoreFields.csp).text("/")
                        .writeEventName(CoreFields.cid).int64(cid)
                        .writeEventName(CoreFields.handler).typedMarshallable(new
                                HeartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalTicks)));
    }

    @Override
    public void writeMarshallable(@NotNull WireOut w) {
        w.write(() -> "heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        w.write(() -> "heartbeatIntervalMs").int64(heartbeatIntervalTicks);
    }

    @Override
    public void processData(@NotNull WireIn inWire, @NotNull WireOut outWire) {
        inWire.read(() -> "heartbeat").text();
    }

    @Override
    public void close() {
        if (closed.getAndSet(true))
            return;
        lastTimeMessageReceived = Long.MAX_VALUE;
        Closeable.closeQuietly(closable());
    }

    public void onMessageReceived() {
        lastTimeMessageReceived = System.currentTimeMillis();
    }

    /**
     * periodically check that messages have been received, ie heartbeats
     */
    private void startHeartbeatCheck() {

        if (hasHeartbeat.getAndSet(true))
            return;

        lastTimeMessageReceived = System.currentTimeMillis();
        HEARTBEAT_EXECUTOR.schedule(() -> {
            if (!hasReceivedHeartbeat() || closed.get()) {
                close();
            } else {
                HEARTBEAT_EXECUTOR.schedule((Runnable) this, heartbeatTimeoutMs, MILLISECONDS);
            }
        }, heartbeatTimeoutMs, MILLISECONDS);
    }

    /**
     * called periodically to check that the heartbeat has been received
     *
     * @return {@code true} if we have received a heartbeat recently
     */
    private boolean hasReceivedHeartbeat() {
        return lastTimeMessageReceived + heartbeatTimeoutMs < System.currentTimeMillis();
    }

}
