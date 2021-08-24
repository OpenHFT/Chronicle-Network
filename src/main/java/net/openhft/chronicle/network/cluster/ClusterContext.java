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
package net.openhft.chronicle.network.cluster;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.ManagedCloseable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.ServerThreadingStrategy;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.threads.BlockingEventLoop;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.EnumSet.of;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.core.threads.HandlerPriority.*;
import static net.openhft.chronicle.threads.EventGroup.CONC_THREADS;

public abstract class ClusterContext<C extends ClusterContext<C, T>, T extends ClusteredNetworkContext<T>>
        extends SelfDescribingMarshallable
        implements Closeable, ManagedCloseable {

    /**
     * Maximum time non-closing threads wait for the ClusterContext
     * to get into CLOSED state when they call close()
     */
    private static final long MAX_CLOSE_WAIT_MS = 5_000;

    enum Status {
        NOT_CLOSED,
        STOPPING,
        CLOSING,
        CLOSED
    }

    // todo should be final
    public static PauserMode DEFAULT_PAUSER_MODE = PauserMode.busy;
    private transient Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private transient Function<C, NetworkStatsListener<T>> networkStatsListenerFactory;
    protected transient EventLoop eventLoop;
    private transient Cluster<T, C> cluster;
    private transient EventLoop acceptorLoop;
    private transient ClusterAcceptorEventHandler<C, T> acceptorEventHandler;
    private final transient TIntObjectMap<HostConnector<T, C>> hostConnectors = new TIntObjectHashMap<>();
    private final transient TIntObjectMap<ConnectionManager<T>> connManagers = new TIntObjectHashMap<>();
    private final transient AtomicReference<Status> status = new AtomicReference<>(Status.NOT_CLOSED);
    private final transient List<java.io.Closeable> closeables = new CopyOnWriteArrayList<>();
    private Function<C, T> networkContextFactory;
    private long heartbeatTimeoutMs = 40_000;
    private long heartbeatIntervalMs = 20_000;
    private Supplier<Pauser> pauserSupplier = DEFAULT_PAUSER_MODE;
    private String affinityCPU;
    private WireType wireType;
    private byte localIdentifier;
    private String localName;
    private ServerThreadingStrategy serverThreadingStrategy;
    private long retryInterval = 1_000L;
    private String procPrefix;

    public ClusterContext() {
        defaults();
    }

    /**
     * Connect to the host specified by the provided {@link HostDetails}. Only attempt connection if the remote host has
     * lower host ID than local - this is required to avoid bidirectional connection establishment
     *
     * @param hd remote host details
     */
    public void connect(HostDetails hd) {
        throwExceptionIfClosed();

        final ConnectionManager<T> connectionManager = new ConnectionManager<>();
        connManagers.put(hd.hostId(), connectionManager);

        if (localIdentifier <= hd.hostId())
            return;

        @NotNull final HostConnector<T, C> hostConnector = new HostConnector<>(castThis(),
                new RemoteConnector<>(tcpEventHandlerFactory()),
                hd.hostId(),
                hd.connectUri());
        closeables.add(hostConnector);
        if (isClosed()) {
            Closeable.closeQuietly(hostConnector);
            return;
        }
        hostConnectors.put(hd.hostId(), hostConnector);

        hostConnector.connect();
    }

    /**
     * Start accepting incoming connections
     *
     * @param hd local host details to accept on
     */
    public void accept(HostDetails hd) {
        throwExceptionIfClosed();

        if (hd.connectUri() == null)
            return;
        acceptorLoop = new BlockingEventLoop(eventLoop(), clusterNamePrefix() + "acceptor-" + localIdentifier);
        try {
            acceptorEventHandler = new ClusterAcceptorEventHandler<>(hd.connectUri(), castThis());

            acceptorLoop.addHandler(acceptorEventHandler);
        } catch (IOException ex) {
            throw new IORuntimeException("Couldn't start replication", ex);
        }
        acceptorLoop.start();
    }

    public ConnectionManager<T> connectionManager(int hostId) {
        throwExceptionIfClosed();
        return connManagers.get(hostId);
    }

    /**
     * Lazily created if not supplier by user
     *
     * @return event loop
     */
    @NotNull
    public EventLoop eventLoop() {
        throwExceptionIfClosed();

        final EventLoop el = this.eventLoop;
        if (el != null)
            return el;
        return synchronizedEventLoop();
    }

    protected synchronized EventLoop synchronizedEventLoop() {
        final EventLoop el = this.eventLoop;
        if (el != null)
            return el;

        return this.eventLoop = new EventGroup(true, pauserSupplier.get(), null, affinityCPU, clusterNamePrefix(), CONC_THREADS,
                of(MEDIUM, TIMER, BLOCKING, REPLICATION, REPLICATION_TIMER));
    }

    @NotNull
    public C eventLoop(EventLoop eventLoop) {
        throwExceptionIfClosed();

        this.eventLoop = eventLoop;
        return castThis();
    }

    public String procPrefix() {
        return procPrefix;
    }

    public void procPrefix(String procPrefix) {
        this.procPrefix = procPrefix;
    }

    public Function<C, NetworkStatsListener<T>> networkStatsListenerFactory() {
        throwExceptionIfClosed();

        return networkStatsListenerFactory;
    }

    @NotNull
    public C networkStatsListenerFactory(Function<C, NetworkStatsListener<T>> networkStatsListenerFactory) {
        this.networkStatsListenerFactory = networkStatsListenerFactory;
        return castThis();
    }

    @NotNull
    public abstract ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory();

    public C serverThreadingStrategy(ServerThreadingStrategy serverThreadingStrategy) {
        this.serverThreadingStrategy = serverThreadingStrategy;
        return castThis();
    }

    public ServerThreadingStrategy serverThreadingStrategy() {
        return serverThreadingStrategy;
    }

    public Cluster<T, C> cluster() {
        return cluster;
    }

    public void cluster(Cluster<T, C> cluster) {
        this.cluster = cluster;
    }

    protected abstract void defaults();

    @NotNull
    public C localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
        return castThis();
    }

    public byte localIdentifier() {
        return localIdentifier;
    }

    public C localName(String localName) {
        this.localName = localName;
        return castThis();
    }

    public String localName() {
        return this.localName;
    }

    @NotNull
    public C wireType(WireType wireType) {
        this.wireType = wireType;
        return castThis();
    }

    public WireType wireType() {
        return wireType;
    }

    @NotNull
    public C heartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return castThis();
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    @NotNull
    public C heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        return castThis();
    }

    public long heartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    /**
     * Sets the {@link Pauser} Supplier to the provided {@code pauserSupplier}. If the user does not supply an {@link #eventLoop()} then the {@code
     * pauserSupplier} is used when lazily creating the {@link EventGroup}.
     *
     * @param pauserSupplier to be used by the event loop
     * @return this ClusterContext
     */
    @NotNull
    public C pauserSupplier(@NotNull Supplier<Pauser> pauserSupplier) {
        this.pauserSupplier = pauserSupplier;
        return castThis();
    }

    public Supplier<Pauser> pauserSupplier() {
        return pauserSupplier;
    }

    public String affinityCPU() {
        return affinityCPU;
    }

    public C affinityCPU(final String affinityCPU) {
        this.affinityCPU = affinityCPU;
        return castThis();
    }

    @NotNull
    public C wireOutPublisherFactory(Function<WireType, WireOutPublisher> wireOutPublisherFactory) {
        this.wireOutPublisherFactory = wireOutPublisherFactory;
        return castThis();
    }

    public Function<WireType, WireOutPublisher> wireOutPublisherFactory() {
        throwExceptionIfClosed();
        return wireOutPublisherFactory;
    }

    @NotNull
    public C networkContextFactory(Function<C, T> networkContextFactory) {
        this.networkContextFactory = networkContextFactory;
        return castThis();
    }

    public Function<C, T> networkContextFactory() {
        throwExceptionIfClosed();
        return networkContextFactory;
    }

    public C retryInterval(final long retryInterval) {
        this.retryInterval = retryInterval;
        return castThis();
    }

    public long retryInterval() {
        return retryInterval;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        networkStatsListenerFactory = wire.read("networkStatsListenerFactory").object(Function.class);
        defaults();
        super.readMarshallable(wire);
    }

    @Override
    public boolean isClosed() {
        return status.get() == Status.CLOSED;
    }

    @Override
    public boolean isClosing() {
        return Status.CLOSING.compareTo(status.get()) <= 0;
    }

    /**
     * For ClusterContext we add a stage called "STOPPING" in which we
     * close the event loops and wait for them to stop. In "STOPPING"
     * phase, {@link #throwExceptionIfClosed()} does not throw. This allows
     * the handlers to finish their last iteration without producing
     * a lot of log noise because the context is closed.
     * <p>
     * This close() method will block until the event loops are finished
     * and everything is closed.
     */
    @Override
    public void close() {
        if (status.compareAndSet(Status.NOT_CLOSED, Status.STOPPING)) {
            performStop();
            status.set(Status.CLOSING);
            performClose();
            status.set(Status.CLOSED);
        } else {
            // Block other threads until we reach CLOSED state, with a reasonable timeout
            long endTime = System.currentTimeMillis() + MAX_CLOSE_WAIT_MS;
            while (status.get() != Status.CLOSED) {
                if (System.currentTimeMillis() > endTime) {
                    Jvm.error().on(ClusterContext.class,
                            format("Waited longer than %,d for context to be closed, continuing (current state: %s)", MAX_CLOSE_WAIT_MS, status.get()));
                    break;
                }
                Jvm.pause(1);
            }
        }
    }

    protected void performStop() {
        acceptorEventHandler.close();
        closeAndWaitForEventLoops(eventLoopsToStop());
    }

    /**
     * Subclasses may override to close additional EventLoops when STOPPING
     *
     * @return The list of event loops to stop before entering CLOSING phase
     */
    protected List<EventLoop> eventLoopsToStop() {
        return Arrays.asList(acceptorLoop, eventLoop);
    }

    protected void performClose() {
        closeQuietly(
                closeables,
                acceptorEventHandler,
                wireOutPublisherFactory,
                networkContextFactory,
                networkStatsListenerFactory);

        closeables.clear();
        acceptorEventHandler = null;
        wireOutPublisherFactory = null;
        networkContextFactory = null;
        networkStatsListenerFactory = null;
        eventLoop = null;
        acceptorLoop = null;
    }

    private void closeAndWaitForEventLoops(@NotNull List<EventLoop> eventLoops) {
        eventLoops.stream()
                .filter(Objects::nonNull)
                .parallel()
                .forEach(Closeable::closeQuietly);
        eventLoops.stream()
                .filter(Objects::nonNull)
                .forEach(this::awaitTerminationQuietly);
    }

    private void awaitTerminationQuietly(@NotNull EventLoop eventLoop) {
        try {
            eventLoop.awaitTermination();
        } catch (Exception e) {
            Jvm.warn().on(ClusterContext.class, "Error waiting for event loop to terminate", e);
        }
    }

    protected abstract String clusterNamePrefix();

    @SuppressWarnings("unchecked")
    private C castThis() {
        return (C) this;
    }
}

