/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.threads.Timer;
import net.openhft.chronicle.core.threads.VanillaEventHandler;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
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

public final class HeartbeatHandler<T extends ClusteredNetworkContext<T>> extends AbstractSubHandler<T> implements
        Demarshallable, WriteMarshallable, HeartbeatEventHandler {

    private static final int MINIMUM_HEARTBEAT_TIMEOUT_MS = 1000;
    private static final int MINIMUM_HEARTBEAT_INTERVAL_MS = 500;
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final AtomicBoolean hasHeartbeats = new AtomicBoolean();
    private final AtomicBoolean closed;
    private volatile long lastTimeMessageReceived;
    private volatile boolean hasTimedOut = false;
    @Nullable
    private ConnectionListener connectionListener;
    @Nullable
    private Timer timer;

    @UsedViaReflection
    public HeartbeatHandler(@NotNull WireIn w) {
        this(w.read("heartbeatTimeoutMs").int64(),
                w.read("heartbeatIntervalMs").int64());
        onMessageReceived();
    }

    private HeartbeatHandler(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        closed = new AtomicBoolean(false);
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        validateHeartbeatParameters(this.heartbeatTimeoutMs, this.heartbeatIntervalMs);
    }

    private static void validateHeartbeatParameters(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        if (heartbeatTimeoutMs <= heartbeatIntervalMs) {
            throw new IllegalArgumentException("Heartbeat timeout must be greater than heartbeat interval, " +
                    "please fix this in your configuration, (heartbeatIntervalMs=" + heartbeatIntervalMs + ", " +
                    "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ")");
        }

        if (heartbeatTimeoutMs < MINIMUM_HEARTBEAT_TIMEOUT_MS) {
            throw new IllegalArgumentException("heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", this is too small (minimum=" + MINIMUM_HEARTBEAT_TIMEOUT_MS + ")");
        }

        if (heartbeatIntervalMs < MINIMUM_HEARTBEAT_INTERVAL_MS) {
            throw new IllegalArgumentException("heartbeatIntervalMs=" + heartbeatIntervalMs + ", this is too small (minimum=" + MINIMUM_HEARTBEAT_INTERVAL_MS + ")");
        }
    }

    public static WriteMarshallable heartbeatHandler(final long heartbeatTimeoutMs,
                                                     final long heartbeatIntervalMs,
                                                     final long cid) {
        validateHeartbeatParameters(heartbeatTimeoutMs, heartbeatIntervalMs);
        return new WriteHeartbeatHandler(cid, heartbeatTimeoutMs, heartbeatIntervalMs);
    }

    @Override
    public void onInitialize(@NotNull WireOut outWire) {

        if (nc().eventLoop().isClosing())
            return;

        if (nc().isAcceptor())
            heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs, cid()).writeMarshallable
                    (outWire);

        @NotNull final WriteMarshallable heartbeatMessage = new HeartbeatMessage();

        connectionListener = nc().acquireConnectionListener();
        timer = new Timer(nc().eventLoop());
        startPeriodicHeartbeatCheck();
        startPeriodicallySendingHeartbeats(heartbeatMessage);
    }

    private void startPeriodicallySendingHeartbeats(WriteMarshallable heartbeatMessage) {

        @NotNull final VanillaEventHandler task = new PeriodicallySendingHeartbeatsHandler(heartbeatMessage);

        timer.scheduleAtFixedRate(task, this.heartbeatIntervalMs, this.heartbeatIntervalMs, nc().periodicPriority());
    }

    @Override
    public boolean isClosed() {
        return closable().isClosed();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut w) {
        w.write("heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        assert heartbeatIntervalMs > 0;
        w.write("heartbeatIntervalMs").int64(heartbeatIntervalMs);
    }

    @Override
    public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
        if (inWire.isEmpty())
            return;
        inWire.read("heartbeat").text();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (connectionListener != null) {
                try {
                    connectionListener.onDisconnected(localIdentifier(), remoteIdentifier(), nc().isAcceptor());
                } catch (Exception e) {
                    Jvm.error().on(getClass(), "Exception thrown by ConnectionListener#onDisconnected", e);
                }
            }
            lastTimeMessageReceived = Long.MAX_VALUE;
        }
    }

    @Override
    public void onMessageReceived() {
        lastTimeMessageReceived = System.currentTimeMillis();
    }

    @Override
    public boolean hasTimedOut() {
        return hasTimedOut;
    }

    private VanillaEventHandler heartbeatCheck() {
        return new HeartbeatCheckHandler();
    }

    /**
     * periodically check that messages have been received, ie heartbeats
     */
    private void startPeriodicHeartbeatCheck() {
        timer.scheduleAtFixedRate(heartbeatCheck(), 0, heartbeatTimeoutMs, nc().periodicPriority());
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

    public String name() {
        return "rid=" + HeartbeatHandler.this.remoteIdentifier() + ", " +
                "lid=" + HeartbeatHandler.this.localIdentifier();
    }

    @Override
    public String toString() {
        return "HeartbeatHandler{" + name() + "}";
    }

    private static class WriteHeartbeatHandler implements WriteMarshallable {
        private final long cid;
        private final long heartbeatTimeoutMs;
        private final long heartbeatIntervalMs;

        public WriteHeartbeatHandler(long cid, long heartbeatTimeoutMs, long heartbeatIntervalMs) {
            this.cid = cid;
            this.heartbeatTimeoutMs = heartbeatTimeoutMs;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut w) {
            w.writeDocument(true,
                    d -> d.writeEventName(CoreFields.csp).text("/")
                            .writeEventName(CoreFields.cid).int64(cid)
                            .writeEventName(CoreFields.handler).typedMarshallable(new
                                    HeartbeatHandler<>(heartbeatTimeoutMs, heartbeatIntervalMs)));
        }

        @Override
        public String toString() {
            return "WriteHeartbeatHandler{" +
                    "cid=" + cid +
                    '}';
        }
    }

    class HeartbeatCheckHandler implements VanillaEventHandler {
        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (HeartbeatHandler.this.isClosed())
                throw InvalidEventHandlerException.reusable();

            boolean hasHeartbeats = HeartbeatHandler.this.hasReceivedHeartbeat();
            boolean prev = HeartbeatHandler.this.hasHeartbeats.getAndSet(hasHeartbeats);

            if (hasHeartbeats != prev) {
                if (!hasHeartbeats) {
                    HeartbeatHandler.this.hasTimedOut = true;
                    throw InvalidEventHandlerException.reusable();
                } else
                    try {
                        if (connectionListener != null) {
                            connectionListener.onConnected(HeartbeatHandler.this.localIdentifier(),
                                    HeartbeatHandler.this.remoteIdentifier(), HeartbeatHandler.this.nc().isAcceptor());
                        }
                    } catch (RuntimeException e) {
                        Jvm.error().on(HeartbeatCheckHandler.class, "Exception thrown by ConnectionListener#onConnected", e);
                    }
            }

            return true;
        }

        @Override
        public String toString() {
            return "HeartbeatCheckHandler{" + name() + "}";
        }
    }

    class PeriodicallySendingHeartbeatsHandler implements VanillaEventHandler {
        private final WriteMarshallable heartbeatMessage;

        public PeriodicallySendingHeartbeatsHandler(WriteMarshallable heartbeatMessage) {
            this.heartbeatMessage = heartbeatMessage;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (HeartbeatHandler.this.isClosed())
                throw InvalidEventHandlerException.reusable();
            // we will only publish a heartbeat if the wire out publisher is empty
            WireOutPublisher wireOutPublisher = HeartbeatHandler.this.nc().wireOutPublisher();
            if (wireOutPublisher.isEmpty())
                wireOutPublisher.publish(heartbeatMessage);
            return true;
        }

        @Override
        public String toString() {
            return "PeriodicallySendingHeartbeatsHandler{" + name() + "}";
        }
    }

    class HeartbeatMessage implements WriteMarshallable {
        @Override
        public void writeMarshallable(@NotNull WireOut w) {
            w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
            w.writeDocument(false, d -> d.write("heartbeat").text(""));
        }

        @Override
        public String toString() {
            return "HeartbeatMessage{" + cid() + "}";
        }
    }
}
