/*
 * Copyright 2016-2020 chronicle.software
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
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.api.session.WritableSubHandler;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.network.HeaderTcpHandler.HANDLER;

public final class UberHandler<T extends ClusteredNetworkContext<T>> extends CspTcpHandler<T> implements
        Demarshallable,
        WriteMarshallable {

    private final int remoteIdentifier;
    private final int localIdentifier;
    @NotNull
    private final AtomicBoolean isClosing = new AtomicBoolean();

    @Nullable
    private ConnectionManager<T> connectionChangedNotifier;

    @UsedViaReflection
    private UberHandler(@NotNull final WireIn wire) {
        remoteIdentifier = wire.read("remoteIdentifier").int32();
        localIdentifier = wire.read("localIdentifier").int32();
        @NotNull final WireType wireType = Objects.requireNonNull(wire.read("wireType").object(WireType.class));
        wireType(wireType);
    }

    private UberHandler(final int localIdentifier,
                        final int remoteIdentifier,
                        @NotNull final WireType wireType) {

        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;

        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;
        wireType(wireType);
    }

    private static String peekContents(@NotNull final DocumentContext dc) {
        try {
            return dc.wire().readingPeekYaml();
        } catch (RuntimeException e) {
            return "Failed to peek at contents due to: " + e.getMessage();
        }
    }

    public static WriteMarshallable uberHandler(int localIdentifier, int remoteIdentifier, WireType wireType) {
        return wire -> {
            try (final DocumentContext ignored = wire.writingDocument(true)) {
                wire.write(() -> HANDLER).typedMarshallable(new UberHandler<>(
                        localIdentifier,
                        remoteIdentifier,
                        wireType));
            }
        };
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
        wire.write("remoteIdentifier").int32(localIdentifier);
        wire.write("localIdentifier").int32(remoteIdentifier);
        wire.write("wireType").object(wireType);
    }

    @Override
    protected void onInitialize() {
        @NotNull final ClusteredNetworkContext<T> nc = nc();
        nc.wireType(wireType());
        isAcceptor(nc.isAcceptor());

        assert checkIdentifierEqualsHostId();
        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;

        @NotNull final WireOutPublisher publisher = nc.wireOutPublisher();
        publisher(publisher);

        @NotNull final EventLoop eventLoop = nc.eventLoop();
        if (!eventLoop.isClosed() && !eventLoop.isClosing()) {
            eventLoop.start();

            // reflect the uber handler
            if (nc().isAcceptor())
                publish(uberHandler(localIdentifier, remoteIdentifier, wireType));

            if (!isClosed())
                notifyConnectionListeners();
        }
    }

    private boolean checkIdentifierEqualsHostId() {
        byte localHostIdentifier = nc().getLocalHostIdentifier();
        if (localIdentifier != localHostIdentifier && localHostIdentifier != 0)
            throw new AssertionError("localId: " + localIdentifier + " != nc().localId: " + localHostIdentifier);
        return true;
    }

    private void notifyConnectionListeners() {
        connectionChangedNotifier = nc().clusterContext().connectionManager(remoteIdentifier);
        if (connectionChangedNotifier != null)
            connectionChangedNotifier.onConnectionChanged(true, nc());
    }

    @Override
    protected void performClose() {
        T nc = nc();
        if (!isClosing.getAndSet(true) && connectionChangedNotifier != null) {
            connectionChangedNotifier.onConnectionChanged(false, nc);
        }

        try {
            if (nc != null) {
                final ConnectionListener listener = nc.acquireConnectionListener();
                if (listener != null)
                    listener.onDisconnected(localIdentifier, remoteIdentifier(), nc.isAcceptor());
            }
        } catch (Exception e) {
            Jvm.error().on(getClass(), "close:", e);
            throw Jvm.rethrow(e);
        }
        Closeable.closeQuietly(writers);
        writers.clear();
        super.performClose();
    }

    @Override
    protected void onRead(@NotNull final DocumentContext dc, @NotNull final WireOut outWire) {
        try {
            if (isClosing.get())
                return;

            // divert to onTouch (block further reads) if a message is already in progress
            {
                final SubHandler<T> handler = handler();
                if (handler != null && handler.inProgress()) {
                    try {
                        if (!handler.onTouch(outWire)) {
                            dc.rollbackOnClose();
                            return;
                        }
                    } catch (Exception e) {
                        removeHandler(handler);
                    }
                    return;
                }
            }

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

                    // trip another read (on the same event) if we're not done yet
                    if (handler.inProgress()) {
                        dc.rollbackOnClose();
                        return;
                    }
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
        } catch (ClosedIllegalStateException e) {
            Jvm.warn().on(getClass(), e);

            close();
        } catch (Throwable e) {
            Jvm.warn().on(getClass(), e);
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
    @SuppressWarnings("ForLoopReplaceableByForEach")
    protected void onWrite(@NotNull final WireOut outWire) {
        super.onWrite(outWire);
        for (int i = 0; i < writers.size(); i++)
            try {
                if (isClosing.get())
                    return;
                final WritableSubHandler<T> w = writers.get(i);
                if (w != null)
                    w.onWrite(outWire);
            } catch (Exception e) {
                Jvm.error().on(getClass(), "onWrite:", e);
                throw Jvm.rethrow(e);
            }
    }

    private void onMessageReceivedOrWritten() {
        final HeartbeatEventHandler heartbeatEventHandler = heartbeatEventHandler();
        if (heartbeatEventHandler != null)
            heartbeatEventHandler.onMessageReceived();
    }

    @Override
    public String toString() {
        return "UberHandler{" +
                "remoteIdentifier=" + remoteIdentifier +
                ", localIdentifier=" + localIdentifier +
                ", isClosing=" + isClosing +
                '}';
    }
}