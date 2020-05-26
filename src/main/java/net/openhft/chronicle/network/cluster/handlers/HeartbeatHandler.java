/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
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
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.threads.Timer;
import net.openhft.chronicle.core.threads.VanillaEventHandler;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.cluster.*;
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

public final class HeartbeatHandler<T extends ClusteredNetworkContext<T>> extends AbstractSubHandler<T> implements
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
        heartbeatTimeoutMs = w.read("heartbeatTimeoutMs").int64();
        heartbeatIntervalMs = w.read("heartbeatIntervalMs").int64();
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
        return new WriteHeartbeatHandler(cid, heartbeatTimeoutMs, heartbeatIntervalMs);
    }

    @Override
    public void onInitialize(@NotNull WireOut outWire) {

        if (nc().eventLoop().isClosed())
            return;

        if (nc().isAcceptor())
            heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs, cid()).writeMarshallable
                    (outWire);

        @NotNull final WriteMarshallable heartbeatMessage = new HeartbeatMessage();

        connectionMonitor = nc().acquireConnectionListener();
        timer = new Timer(nc().eventLoop());
        startPeriodicHeartbeatCheck();
        startPeriodicallySendingHeartbeats(heartbeatMessage);
    }

    private void startPeriodicallySendingHeartbeats(WriteMarshallable heartbeatMessage) {

        @NotNull final VanillaEventHandler task = new PeriodicallySendingHeartbeatsHandler(heartbeatMessage);

        timer.scheduleAtFixedRate(task, this.heartbeatIntervalMs, this.heartbeatIntervalMs);
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
        if (connectionMonitor != null)
            connectionMonitor.onDisconnected(localIdentifier(), remoteIdentifier(), nc().isAcceptor());
        lastTimeMessageReceived = Long.MAX_VALUE;
        Closeable closable = closable();
        if (closable != null && !closable.isClosed()) {
            Closeable.closeQuietly(closable);
        }
    }

    @Override
    public void onMessageReceived() {
        lastTimeMessageReceived = System.currentTimeMillis();
    }

    private VanillaEventHandler heartbeatCheck() {
        return new HeartbeatCheckHandler();
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

    public static class Factory<T extends ClusteredNetworkContext<T>> implements Function<ClusterContext<T>, WriteMarshallable>,
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

            if (HeartbeatHandler.this.closable().isClosed())
                throw new InvalidEventHandlerException("closed");

            boolean hasHeartbeats = HeartbeatHandler.this.hasReceivedHeartbeat();
            boolean prev = HeartbeatHandler.this.hasHeartbeats.getAndSet(hasHeartbeats);

            if (hasHeartbeats != prev) {
                if (!hasHeartbeats) {
                    connectionMonitor.onDisconnected(HeartbeatHandler.this.localIdentifier(),
                            HeartbeatHandler.this.remoteIdentifier(), HeartbeatHandler.this.nc().isAcceptor());

                    HeartbeatHandler.this.close();

                    final Runnable socketReconnector = HeartbeatHandler.this.nc().socketReconnector();

                    // if we have been terminated then we should not attempt to reconnect
                    TerminationEventHandler<T> teHandler = HeartbeatHandler.this.nc().terminationEventHandler();
                    if (teHandler != null && teHandler.isTerminated() && socketReconnector != null)
                        socketReconnector.run();

                    throw new InvalidEventHandlerException("closed");
                } else
                    connectionMonitor.onConnected(HeartbeatHandler.this.localIdentifier(),
                            HeartbeatHandler.this.remoteIdentifier(), HeartbeatHandler.this.nc().isAcceptor());
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
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            if (HeartbeatHandler.this.isClosed())
                throw new InvalidEventHandlerException("closed");
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