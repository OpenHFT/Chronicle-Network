/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.api.session.WritableSubHandler;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.cluster.handlers.Registerable;
import net.openhft.chronicle.network.cluster.handlers.RejectedHandlerException;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.network.test.TestClusterContext;
import net.openhft.chronicle.network.test.TestClusteredNetworkContext;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static net.openhft.chronicle.network.HeaderTcpHandler.HANDLER;
import static net.openhft.chronicle.network.cluster.handlers.UberHandler.uberHandler;
import static net.openhft.chronicle.network.connection.CoreFields.csp;
import static net.openhft.chronicle.network.test.TestClusterContext.forHosts;
import static org.junit.jupiter.api.Assertions.*;

class UberHandlerTest extends NetworkTestCommon {

    private static final int SIZE_OF_BIG_PAYLOAD = 200 * 1024;
    private static final int ROUNDS_PER_SIZE_CYCLE = 100;
    private static final int NUM_HANDLERS = 4;
    private static final long RUN_TIME_MS = 5_000;
    private static final int FIRST_CID = 12345;
    private static final String TEST_HANDLERS_CSP = "testhandlercsp";
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicInteger STOPPED = new AtomicInteger(0);
    private static final List<Long> MESSAGES_RECEIVED_CIDS = new ArrayList<>();
    private static final AtomicInteger SENDERS_INITIALIZED = new AtomicInteger();
    private static final Map<Long, Integer> COUNTERS_PER_CID = new ConcurrentHashMap<>();
    private static final AtomicBoolean REJECTED_HANDLER_ONREAD_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean REJECTED_HANDLER_ONWRITE_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean REJECTING_SUB_HANDLER_SHOULD_REJECT = new AtomicBoolean(false);
    private static final AtomicReference<Map<Object, RegisterableSubHandler>> REGISTRY = new AtomicReference<>();

    @BeforeEach
    void before() {
        YamlLogging.setAll(false);
        System.setProperty("TcpEventHandler.tcpBufferSize", "131072");
        COUNTERS_PER_CID.clear();
        RUNNING.set(true);
        STOPPED.set(0);
        MESSAGES_RECEIVED_CIDS.clear();
        SENDERS_INITIALIZED.set(0);
        REJECTING_SUB_HANDLER_SHOULD_REJECT.set(false);
        REJECTED_HANDLER_ONREAD_CALLED.set(false);
        REJECTED_HANDLER_ONWRITE_CALLED.set(false);
        REGISTRY.set(null);
    }

    @Override
    @BeforeEach
    protected void threadDump() {
        super.threadDump();
    }

    @AfterEach
    public void after() {
        System.clearProperty("TcpEventHandler.tcpBufferSize");
    }

    @Test
    void testUberHandlerWithMultipleSubHandlersAndHeartbeats() throws IOException, TimeoutException {
        TCPRegistry.createServerSocketChannelFor("initiator", "acceptor");
        HostDetails initiatorHost = new HostDetails().hostId(2).connectUri("initiator");
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri("acceptor");

        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                if (isConnected) {
                    IntStream.range(0, NUM_HANDLERS).forEach(seq -> {
                        nc.wireOutPublisher().publish(w ->
                                w.writeDocument(true, d -> sendHandler(d, FIRST_CID + seq, new PingPongHandler(false))));
                    });
                }
            });

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < RUN_TIME_MS && !Thread.currentThread().isInterrupted()) {
                Jvm.pause(100);
            }
            Jvm.startup().on(PingPongHandler.class, "Test complete, stopping handlers countersPerCid=" + COUNTERS_PER_CID);
            stopAndWaitTillAllHandlersEnd();
            assertTrue(pingPongsAllCompletedAtLeastOneRound());
        }
    }

    @Test
    void constructorWillThrowIfLocalAndRemoteIdentifiersAreTheSame() {
        Wire wire = new BinaryWire(Bytes.allocateElasticOnHeap());
        final WriteMarshallable writeMarshallable = uberHandler(123, 123, WireType.BINARY);
        assertThrows(IllegalArgumentException.class, () -> writeMarshallable.writeMarshallable(wire));
    }

    private void stopAndWaitTillAllHandlersEnd() throws TimeoutException {
        RUNNING.set(false);
        TimingPauser pauser = Pauser.balanced();

        // Wait till both sides of all handlers are stopped
        while (STOPPED.get() < NUM_HANDLERS * 2) {
            pauser.pause(10, TimeUnit.SECONDS);
        }
    }

    private boolean pingPongsAllCompletedAtLeastOneRound() {
        return COUNTERS_PER_CID.size() == NUM_HANDLERS && COUNTERS_PER_CID.values().stream().allMatch(val -> val > 0);
    }

    @Test
    void testHandlerWillCloseWhenHostIdsAreWrong() throws IOException {
        expectException("Received a handler for host ID: 98, my host ID is: 1 this is probably a configuration error");
        ignoreException("Closed");
        ignoreException("SubHandler HeartbeatHandler");

        TCPRegistry.createServerSocketChannelFor("initiator", "acceptor");
        HostDetails initiatorHost = new HostDetails().hostId(99).connectUri("initiator");
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri("acceptor");
        HostDetails acceptorHostWithInvalidId = new HostDetails().hostId(98).connectUri("acceptor");

        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHostWithInvalidId)) {

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            AtomicBoolean establishedConnection = new AtomicBoolean(false);
            initiatorCtx.connectionManager(acceptorHostWithInvalidId.hostId()).addListener((nc, isConnected) -> {
                if (isConnected) {
                    establishedConnection.set(true);
                }
            });
            Jvm.pause(2000);
            assertFalse(establishedConnection.get());
        }
    }

    @Test
    void newConnectionListenersAreExecutedOnEventLoopForExistingConnections() throws IOException, TimeoutException {
        TCPRegistry.createServerSocketChannelFor("initiator", "acceptor");
        HostDetails initiatorHost = new HostDetails().hostId(2).connectUri("initiator");
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri("acceptor");

        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            // Block until the connection is established
            AtomicBoolean establishedConnection = new AtomicBoolean(false);
            AtomicReference<NetworkContext<?>> networkContext = new AtomicReference<>();
            initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                if (isConnected) {
                    establishedConnection.set(true);
                    networkContext.set(nc);
                }
            });
            TimingPauser pauser = Pauser.balanced();
            while (!establishedConnection.get()) {
                pauser.pause(3, TimeUnit.SECONDS);
            }

            // Add a new connection listener to the already established connection, ensure it executes on the event loop
            AtomicBoolean executedOnEventLoop = new AtomicBoolean(false);
            initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                if (isConnected) {
                    assertSame(networkContext.get(), nc);
                    executedOnEventLoop.set(EventLoop.inEventLoop());
                }
            });

            pauser.reset();
            while (!executedOnEventLoop.get()) {
                pauser.pause(3, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Tests that multiple writers write first in a round-robin fashion
     * <p>
     * e.g. for 4 handlers:
     * 1, 2, 3, 4
     * 4, 1, 2, 3
     * 3, 4, 1, 2
     * 2, 3, 4, 1
     * ...
     * <p>
     * This is fairer than e.g.
     * 1, 2, 3, 4
     * 1, 2, 3, 4
     * ...
     * where 1 gets to fill the buffer every time and can starve 2, 3, 4
     */
    @Test
    void testBusyWritingHandlersAreCalledFirstInRoundRobin() throws IOException, TimeoutException {
        TCPRegistry.createServerSocketChannelFor("initiator", "acceptor");
        HostDetails initiatorHost = new HostDetails().hostId(2).connectUri("initiator");
        HostDetails acceptorHost = new HostDetails().hostId(1).connectUri("acceptor");

        try (TestClusterContext acceptorCtx = forHosts(acceptorHost, initiatorHost);
             TestClusterContext initiatorCtx = forHosts(initiatorHost, acceptorHost)) {

            acceptorCtx.cluster().start(acceptorHost.hostId());
            initiatorCtx.cluster().start(initiatorHost.hostId());

            initiatorCtx.connectionManager(acceptorHost.hostId()).addListener((nc, isConnected) -> {
                if (isConnected) {
                    IntStream.range(0, NUM_HANDLERS).forEach(seq -> {
                        nc.wireOutPublisher().publish(w ->
                                w.writeDocument(true, d -> sendHandler(d, FIRST_CID + seq, new Receiver())));
                    });
                }
            });

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < RUN_TIME_MS && !Thread.currentThread().isInterrupted()) {
                Jvm.pause(100);
            }
            Jvm.startup().on(UberHandlerTest.class, String.format("Test complete, stopping handlers messages received=%,d", MESSAGES_RECEIVED_CIDS.size()));
            stopAndWaitTillAllHandlersEnd();
            assertTrue(messagesWereReceivedInRoundRobinOrder());
        }
    }

    @Test
    void rejectedOnInitializeHandlersAreRemovedFromReadAndWrite() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            expectException("Rejected in onInitialize");
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            testHarness.registerSubHandler(new WritableRejectingSubHandler());
            // This message will be silently dropped
            testHarness.sendMessageToCurrentHandler();
            testHarness.callProcess();
            assertFalse(REJECTED_HANDLER_ONREAD_CALLED.get());
            assertFalse(REJECTED_HANDLER_ONWRITE_CALLED.get());
        }
    }

    @Test
    void rejectedOnReadHandlersAreRemoveFromReadAndWrite() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new WritableRejectingSubHandler());
            expectException("Rejected in onRead");
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            testHarness.sendMessageToCurrentHandler();
            // This message will be silently dropped
            testHarness.sendMessageToCurrentHandler();
            testHarness.callProcess();
            assertFalse(REJECTED_HANDLER_ONWRITE_CALLED.get());
            assertFalse(REJECTED_HANDLER_ONREAD_CALLED.get());
        }
    }

    @Test
    void rejectedOnWriteHandlersAreRemoveFromReadAndWrite() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new WritableRejectingSubHandler());
            expectException("Rejected in onWrite");
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            testHarness.callProcess();
            // This message will be silently dropped
            testHarness.sendMessageToCurrentHandler();
            testHarness.callProcess();
            assertFalse(REJECTED_HANDLER_ONWRITE_CALLED.get());
            assertFalse(REJECTED_HANDLER_ONREAD_CALLED.get());
        }
    }

    @Test
    void addHandlerRegistersRegisterableHandlers() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new RegisterableSubHandler());
            assertEquals(REGISTRY.get().get(RegisterableSubHandler.REGISTRY_KEY).getClass(), RegisterableSubHandler.class);
        }
    }

    @Test
    void removeHandlerUnregistersRegisterableHandlers() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new RegisterableSubHandler());
            assertEquals(REGISTRY.get().get(RegisterableSubHandler.REGISTRY_KEY).getClass(), RegisterableSubHandler.class);
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            expectException("Rejected in onRead");
            testHarness.sendMessageToCurrentHandler();
            assertTrue(REGISTRY.get().isEmpty());
        }
    }

    @Test
    void addHandlerAddsConnectionListenerHandlersToNetworkContext() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new ConnectionListenerSubHandler());
            assertEquals(1, testHarness.nc().connectionListeners.size());
        }
    }

    @Test
    void removeHandlerRemovesConnectionListenerHandlersFromNetworkContext() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new ConnectionListenerSubHandler());
            assertEquals(1, testHarness.nc().connectionListeners.size());
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            expectException("Rejected in onRead");
            testHarness.sendMessageToCurrentHandler();
            assertEquals(0, testHarness.nc().connectionListeners.size());
        }
    }

    @Test
    void warningIsLoggedWhenRemovedHandlersAreSwitchedTo() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            final int removedHandlerCid = 1234;
            testHarness.registerSubHandler(new RejectingSubHandler(), removedHandlerCid, "test/removed-handler");
            REJECTING_SUB_HANDLER_SHOULD_REJECT.set(true);
            expectException("Rejected in onRead");
            testHarness.sendMessageToCurrentHandler();
            expectException("Installing no-op handler to drop messages for removed handler (cid=1234, csp=test/removed-handler)");
            testHarness.switchToSubHandler(removedHandlerCid);
            // This should be silently ignored
            testHarness.sendMessageToCurrentHandler();
        }
    }

    @Test
    void warningIsLoggedWhenNonExistentHandlerIsSwitchedToAndMessagesReceived() {
        try (final UberHandlerTestHarness testHarness = new UberHandlerTestHarness()) {
            testHarness.registerSubHandler(new RejectingSubHandler(), 456, "existing-handler");
            expectException("handler not found : for CID=1234, known cids={456");
            testHarness.switchToSubHandler(1234);
            expectException("Dropped message because handler == null, check that the Csp/Cid has been sent. (lastCid=1234)");
            testHarness.sendMessageToCurrentHandler();
        }
    }

    private static class ConnectionListenerSubHandler extends RejectingSubHandler implements ConnectionListener {

        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
        }
    }

    private static class RegisterableSubHandler extends RejectingSubHandler implements Registerable<RegisterableSubHandler> {

        public static final String REGISTRY_KEY = "registryKey";

        @Override
        public Object registryKey() {
            return REGISTRY_KEY;
        }

        @Override
        public void registry(Map<Object, RegisterableSubHandler> registry) {
            REGISTRY.set(registry);
        }
    }

    private static class WritableRejectingSubHandler extends RejectingSubHandler implements WritableSubHandler<TestClusteredNetworkContext> {

        @Override
        public void onWrite(WireOut outWire) {
            if (!rejected && REJECTING_SUB_HANDLER_SHOULD_REJECT.get()) {
                rejected = true;
                throw new RejectedHandlerException("Rejected in onWrite");
            } else if (rejected) {
                REJECTED_HANDLER_ONWRITE_CALLED.set(true);
            }
        }
    }

    private static class RejectingSubHandler extends AbstractSubHandler<TestClusteredNetworkContext> implements Marshallable {

        protected boolean rejected = false;

        @Override
        public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
            if (!rejected && REJECTING_SUB_HANDLER_SHOULD_REJECT.get()) {
                rejected = true;
                throw new RejectedHandlerException("Rejected in onRead");
            } else if (rejected) {
                REJECTED_HANDLER_ONREAD_CALLED.set(true);
            }
        }

        @Override
        public void onInitialize(WireOut outWire) throws RejectedExecutionException {
            if (!rejected && REJECTING_SUB_HANDLER_SHOULD_REJECT.get()) {
                rejected = true;
                throw new RejectedHandlerException("Rejected in onInitialize");
            }
        }
    }

    private boolean messagesWereReceivedInRoundRobinOrder() {
        long offset = MESSAGES_RECEIVED_CIDS.get(0) - FIRST_CID;
        for (int i = 0; i < MESSAGES_RECEIVED_CIDS.size(); i++) {
            long expectedCID = FIRST_CID + ((i + offset) % NUM_HANDLERS);
            if (expectedCID != MESSAGES_RECEIVED_CIDS.get(i)) {
                return false;
            }
            if (i % NUM_HANDLERS == NUM_HANDLERS - 1)
                offset++;
        }
        return true;
    }

    private void sendHandler(WireOut wireOut, int cid, Marshallable handler) {
        wireOut.writeEventName(csp).text(TEST_HANDLERS_CSP)
                .writeEventName(CoreFields.cid).int64(cid)
                .writeEventName(CoreFields.handler).typedMarshallable(handler);
    }

    static class Sender extends AbstractCompleteFlaggingHandler
            implements WritableSubHandler<TestClusteredNetworkContext>, Marshallable {

        public Sender() {
        }

        @Override
        public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
            throw new AssertionError("The Sender should never receive any messages, the test depends that it is solely a writer");
        }

        @Override
        public void onInitialize(WireOut outWire) throws RejectedExecutionException {
            Jvm.startup().on(Sender.class, "Initializing (cid=" + cid() + ")");
            SENDERS_INITIALIZED.incrementAndGet();
        }

        @Override
        public void onWrite(WireOut outWire) {
            if (!RUNNING.get()) {
                flagComplete();
                return;
            }

            if (SENDERS_INITIALIZED.get() != NUM_HANDLERS) {
                return;
            }

            try (final DocumentContext dc = outWire.writingDocument(true)) {
                dc.wire().write(CoreFields.cid).int64(cid());
            }
            try (final DocumentContext documentContext = outWire.writingDocument()) {
                documentContext.wire().write("hello").text("world");
            }
        }
    }

    static class Receiver extends AbstractCompleteFlaggingHandler
            implements WritableSubHandler<TestClusteredNetworkContext>, Marshallable {

        @Override
        public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
            MESSAGES_RECEIVED_CIDS.add(cid());
        }

        @Override
        public void onInitialize(WireOut outWire) throws RejectedExecutionException {
            Jvm.startup().on(Receiver.class, "Initializing (cid=" + cid() + ")");
            outWire.writeDocument(true, d ->
                    d.writeEventName(CoreFields.csp).text(TEST_HANDLERS_CSP)
                            .writeEventName(CoreFields.cid).int64(cid())
                            .writeEventName(CoreFields.handler).typedMarshallable(new Sender())
            );
        }

        @Override
        public void onWrite(WireOut outWire) {
            if (!RUNNING.get()) {
                flagComplete();
            }
        }
    }

    static class PingPongHandler extends AbstractCompleteFlaggingHandler implements
            Marshallable, WritableSubHandler<TestClusteredNetworkContext> {

        private static final int LOGGING_INTERVAL = 50;

        private boolean initiator;
        private boolean initiated = false;
        private int round = 0;

        public PingPongHandler(boolean initiator) {
            this.initiator = initiator;
        }

        @Override
        public void onInitialize(WireOut outWire) throws RejectedExecutionException {
            Jvm.startup().on(PingPongHandler.class, "Initializing PingPongHandler (cid=" + cid() + ", initiator=" + initiator + ")");
            // Send back the handler for the other side
            if (!initiator) {
                outWire.writeDocument(true, d ->
                        d.writeEventName(CoreFields.csp).text(TEST_HANDLERS_CSP)
                                .writeEventName(CoreFields.cid).int64(cid())
                                .writeEventName(CoreFields.handler).typedMarshallable(new PingPongHandler(!initiator))
                );
            }
        }

        @Override
        public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
            throwExceptionIfClosed();
            if (!RUNNING.get()) {
                flagComplete();
                return;
            }

            final StringBuilder eventName = Wires.acquireStringBuilder();
            final ValueIn valueIn = inWire.readEventName(eventName);

            sendPingPongCid(outWire);
            try (final DocumentContext dc = outWire.writingDocument(false)) {

                // Keep track of what round we're up to
                if (initiator) {
                    COUNTERS_PER_CID.put(cid(), round);
                }

                round++;
                if (round % LOGGING_INTERVAL == 0) {
                    Jvm.startup().on(PingPongHandler.class, "PingPongHandler at round " + round + "(cid=" + cid() + ", initiator=" + initiator + ")");
                }
                if ("ping".equals(eventName.toString())) {
                    assert valueIn.bytes().length == inWire.read("bytesLength").int32();
                    writeRandomJunk("pong", dc, round);
                } else if ("pong".equals(eventName.toString())) {
                    assert valueIn.bytes().length == inWire.read("bytesLength").int32();
                    writeRandomJunk("ping", dc, round);
                } else {
                    throw new IllegalStateException("Got unknown event: " + eventName);
                }
            }
        }

        private void writeRandomJunk(String eventName, DocumentContext dc, int counter) {
            Bytes<?> bigJunk = Bytes.allocateElasticOnHeap();
            int payloadSize = (int) ((round / (float) ROUNDS_PER_SIZE_CYCLE) * SIZE_OF_BIG_PAYLOAD);
            for (int i = 0; i < payloadSize; i++) {
                bigJunk.writeByte((byte) i);
            }
            dc.wire().write(eventName)
                    .bytes(bigJunk)
                    .write("bytesLength").int32(payloadSize)
                    .write("counter").int32(counter);
        }

        @Override
        public void onWrite(WireOut outWire) {
            throwExceptionIfClosed();
            if (!RUNNING.get()) {
                flagComplete();
                return;
            }

            if (initiator && !initiated) {
                sendPingPongCid(outWire);
                try (final DocumentContext dc = outWire.writingDocument()) {
                    writeRandomJunk("ping", dc, round++);
                }
                initiated = true;
            }
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("initiator").bool(initiator);
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

    /**
     * Just a common way of knowing when all handlers have stopped writing
     */
    abstract static class AbstractCompleteFlaggingHandler extends AbstractSubHandler<TestClusteredNetworkContext> {
        private boolean flaggedComplete = false;

        protected void flagComplete() {
            if (!flaggedComplete) {
                STOPPED.incrementAndGet();
                flaggedComplete = true;
                Jvm.startup().on(getClass(), String.format("Handler with cid=%s finished", cid()));
            }
        }
    }

    static class UberHandlerTestHarness extends AbstractCloseable {

        private final TestClusterContext clusterContext;
        private final TestClusteredNetworkContext nc;
        private final UberHandler<TestClusteredNetworkContext> uberHandler;
        private final Wire inWire;
        private final Wire outWire;

        public UberHandlerTestHarness() {
            clusterContext = new TestClusterContext();
            nc = new TestClusteredNetworkContext(clusterContext);
            nc.wireOutPublisher(new VanillaWireOutPublisher(clusterContext.wireType()));
            uberHandler = createHandler();
            uberHandler.nc(nc);
            inWire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
            outWire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
        }

        private UberHandler<TestClusteredNetworkContext> createHandler() {
            Wire wire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
            uberHandler(123, 456, WireType.BINARY).writeMarshallable(wire);
            try (final DocumentContext documentContext = wire.readingDocument()) {
                return wire.read(HANDLER).object(UberHandler.class);
            }
        }

        private void registerSubHandler(WriteMarshallable subHandler) {
            registerSubHandler(subHandler, 12345L, "12345");
        }

        private void registerSubHandler(WriteMarshallable subHandler, long cid, String csp) {
            try (final DocumentContext documentContext = inWire.writingDocument(true)) {
                final Wire documentWire = documentContext.wire();
                documentWire.write(CoreFields.csp).text(csp);
                documentWire.write(CoreFields.cid).int64(cid);
                documentWire.write(CoreFields.handler).typedMarshallable(subHandler);
            }
            uberHandler.process(inWire.bytes(), outWire.bytes(), nc);
        }

        public void callProcess() {
            uberHandler.process(inWire.bytes(), outWire.bytes(), nc);
        }

        private void sendMessageToCurrentHandler() {
            try (final DocumentContext documentContext = inWire.writingDocument(false)) {
                final Wire documentWire = documentContext.wire();
                documentWire.write("junk").text("to trigger an onRead");
            }
            uberHandler.process(inWire.bytes(), outWire.bytes(), nc);
        }

        private void switchToSubHandler(long cid) {
            try (final DocumentContext documentContext = inWire.writingDocument(true)) {
                final Wire documentWire = documentContext.wire();
                documentWire.write(CoreFields.cid).int64(cid);
            }
            uberHandler.process(inWire.bytes(), outWire.bytes(), nc);
        }

        public TestClusteredNetworkContext nc() {
            return nc;
        }

        @Override
        protected void performClose() throws IllegalStateException {
            Closeable.closeQuietly(clusterContext, nc, uberHandler, inWire, outWire);
        }
    }
}
