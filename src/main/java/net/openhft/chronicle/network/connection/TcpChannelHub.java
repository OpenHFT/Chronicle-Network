/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ConnectionDroppedException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.WanSimulator;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMonitor;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Integer.getInteger;
import static java.lang.ThreadLocal.withInitial;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.*;
import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;

/**
 * The TcpChannelHub is used to send your messages to the server and then read the servers response.
 * The TcpChannelHub ensures that each response is marshalled back onto the appropriate client
 * thread. It does this through the use of a unique transaction ID ( we call this transaction ID the
 * "tid" ), when the server responds to the client, its expected that the server sends back the tid
 * as the very first field in the message. The TcpChannelHub will look at each message and read the
 * tid, and then marshall the message onto your appropriate client thread. Created by Rob Austin
 */
public class TcpChannelHub implements Closeable {

    public static final int TCP_BUFFER = getTcpBufferSize();
    public static final int SAFE_TCP_SIZE = TCP_BUFFER * 3 / 4;
    private static final int HEATBEAT_PING_PERIOD =
            getInteger("heartbeat.ping.period",
                    Jvm.isDebug() ? 30_000 : 5_000);
    private static final int HEATBEAT_TIMEOUT_PERIOD =
            getInteger("heartbeat.timeout",
                    Jvm.isDebug() ? 120_000 : 15_000);
    private static final int SIZE_OF_SIZE = 4;
    private static final Set<TcpChannelHub> hubs = new CopyOnWriteArraySet<>();
    private static final Logger LOG = LoggerFactory.getLogger(TcpChannelHub.class);
    final long timeoutMs;
    @NotNull
    private final String name;
    private final int tcpBufferSize;
    private final Wire outWire;
    @NotNull
    private final SocketAddressSupplier socketAddressSupplier;
    private final Set<Long> preventSubscribeUponReconnect = new ConcurrentSkipListSet<>();
    private final ReentrantLock outBytesLock = TraceLock.create();
    private final Condition condition = outBytesLock.newCondition();
    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);
    @NotNull
    private final SessionProvider sessionProvider;
    @NotNull
    private final TcpSocketConsumer tcpSocketConsumer;
    @NotNull
    private final EventLoop eventLoop;
    @NotNull
    private final WireType wireType;
    private final Wire handShakingWire;
    @Nullable
    private final ClientConnectionMonitor clientConnectionMonitor;
    private final ConnectionStrategy connectionStrategy;
    @NotNull
    private Pauser pauser = new LongPauser(100, 100, 500, 20_000, TimeUnit.MICROSECONDS);
    // private final String description;
    private long largestChunkSoFar = 0;
    @Nullable
    private volatile SocketChannel clientChannel;
    private volatile boolean closed;
    @NotNull
    private CountDownLatch receivedClosedAcknowledgement = new CountDownLatch(1);
    // set up in the header
    private long limitOfLast = 0;
    private boolean shouldSendCloseMessage;
    private HandlerPriority priority;
    public TcpChannelHub(@Nullable final SessionProvider sessionProvider,
                         @NotNull final EventLoop eventLoop,
                         @NotNull final WireType wireType,
                         @NotNull final String name,
                         @NotNull final SocketAddressSupplier socketAddressSupplier,
                         boolean shouldSendCloseMessage,
                         @Nullable ClientConnectionMonitor clientConnectionMonitor,
                         @NotNull final HandlerPriority monitor,
                         @NotNull final ConnectionStrategy connectionStrategy) {
        assert !name.trim().isEmpty();
        this.connectionStrategy = connectionStrategy;
        this.priority = monitor;
        this.socketAddressSupplier = socketAddressSupplier;
        this.eventLoop = eventLoop;
        this.tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", TCP_BUFFER);
        this.outWire = wireType.apply(elasticByteBuffer());
        // this.inWire = wireType.apply(elasticByteBuffer());
        this.name = name.trim();
        this.timeoutMs = Integer.getInteger("tcp.client.timeout", 10_000);
        this.wireType = wireType;

        // we are always going to send the header as text wire, the server will
        // respond in the wire define by the wireType field, all subsequent types must be in wireType
        this.handShakingWire = WireType.TEXT.apply(Bytes.elasticByteBuffer());

        this.sessionProvider = sessionProvider;
        this.shouldSendCloseMessage = shouldSendCloseMessage;
        this.clientConnectionMonitor = clientConnectionMonitor;
        hubs.add(this);
        eventLoop.addHandler(new PauserMonitor(pauser, "async-read", 30));

        // has to be done last as it starts a thread which uses this class.
        this.tcpSocketConsumer = new TcpSocketConsumer();
    }

    private static int getTcpBufferSize() {
        String sizeStr = System.getProperty("TcpEventHandler.tcpBufferSize");
        if (sizeStr != null && !sizeStr.isEmpty())
            try {
                int size = Integer.parseInt(sizeStr);
                if (size >= 64 << 10)
                    return size;
            } catch (Exception e) {
                Jvm.warn().on(TcpChannelHub.class, "Unable to parse tcpBufferSize=" + sizeStr, e);
            }
        try {
            try (ServerSocket ss = new ServerSocket(0)) {
                try (Socket s = new Socket("localhost", ss.getLocalPort())) {
                    s.setReceiveBufferSize(4 << 20);
                    s.setSendBufferSize(4 << 20);
                    int size = Math.min(s.getReceiveBufferSize(), s.getSendBufferSize());
                    (size >= 128 << 10 ? Jvm.debug() : Jvm.warn())
                            .on(TcpChannelHub.class, "tcpBufferSize = " + size / 1024.0 + " KiB");
                    return size;
                }
            }
        } catch (Exception e) {
            throw new IORuntimeException(e); // problem with networking subsystem.
        }
    }

    public static void assertAllHubsClosed() {
        @NotNull StringBuilder errors = new StringBuilder();
        for (@NotNull TcpChannelHub h : hubs) {
            if (!h.isClosed())
                errors.append("Connection ").append(h).append(" still open\n");
            h.close();
        }
        hubs.clear();
        if (errors.length() > 0)
            throw new AssertionError(errors.toString());
    }

    public static void closeAllHubs() {
        @NotNull final TcpChannelHub[] hubsArr = hubs.toArray(new TcpChannelHub[hubs.size()]);
        for (@NotNull TcpChannelHub hub : hubsArr) {
            if (hub.isClosed())
                continue;

            if (Jvm.isDebugEnabled(TcpChannelHub.class))
                Jvm.debug().on(TcpChannelHub.class, "Closing " + hub);
            hub.close();
        }
        hubs.clear();
    }

    private static void logToStandardOutMessageReceived(@NotNull Wire wire) {
        @NotNull final Bytes<?> bytes = wire.bytes();

        if (!YamlLogging.showClientReads())
            return;

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {
            try {

                //         LOG.info("Bytes.toString(bytes)=" + Bytes.toString(bytes));
                LOG.info("\nreceives:\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(wire) +
                        "```\n");
                YamlLogging.title = "";
                YamlLogging.writeMessage("");

            } catch (Exception e) {
                Jvm.warn().on(TcpChannelHub.class, Bytes.toString(bytes), e);
            }
        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    private static void logToStandardOutMessageReceivedInERROR(@NotNull Wire wire) {
        @NotNull final Bytes<?> bytes = wire.bytes();

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {
            try {

                LOG.info("\nreceives IN ERROR:\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(wire) +
                        "```\n");
                YamlLogging.title = "";
                YamlLogging.writeMessage("");

            } catch (Exception e) {
                String x = Bytes.toString(bytes);
                Jvm.warn().on(TcpChannelHub.class, x, e);
            }
        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    private static boolean checkWritesOnReadThread(@NotNull TcpSocketConsumer tcpSocketConsumer) {
        assert Thread.currentThread() != tcpSocketConsumer.readThread : "if writes" +
                " and reads are on the same thread this can lead " +
                "to deadlocks with the server, if the server buffer becomes full";
        return true;
    }

    void clear(@NotNull final Wire wire) {
        assert wire.startUse();
        try {
            wire.clear();
        } finally {
            assert wire.endUse();
        }
    }

    @Nullable
    SocketChannel openSocketChannel(InetSocketAddress socketAddress) throws IOException {
        final SocketChannel result = SocketChannel.open();
        @Nullable Selector selector = null;
        boolean failed = true;
        try {
            result.configureBlocking(false);
            Socket socket = result.socket();
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(tcpBufferSize);
            socket.setSendBufferSize(tcpBufferSize);
            socket.setSoTimeout(0);
            socket.setSoLinger(false, 0);
            result.connect(socketAddress);

            selector = Selector.open();
            result.register(selector, SelectionKey.OP_CONNECT);

            int select = selector.select(2500);
            if (select == 0) {
                Jvm.warn().on(getClass(), "Timed out attempting to connect to " + socketAddress);
                return null;
            } else {
                try {
                    if (!result.finishConnect())
                        return null;

                } catch (IOException e) {
                    if (Jvm.isDebugEnabled(getClass()))
                        Jvm.debug().on(getClass(), "Failed to connect to " + socketAddress + " " + e);
                    return null;
                }
            }
            failed = false;
            return result;

        } finally {
            Closeable.closeQuietly(selector);
            if (failed)
                Closeable.closeQuietly(result);
        }
    }

    /**
     * prevents subscriptions upon reconnect for the following {@code tid} its useful to call this
     * method when an unsubscribe has been sent to the server, but before the server has acknoleged
     * the unsubscribe, hence, perverting a resubscribe upon reconnection.
     *
     * @param tid unique transaction id
     */
    public void preventSubscribeUponReconnect(long tid) {
        preventSubscribeUponReconnect.add(tid);
    }

    @NotNull
    @Override
    public String toString() {
        return "TcpChannelHub{" +
                "name=" + name +
                "remoteAddressSupplier=" + socketAddressSupplier + '}';
    }

    private void onDisconnected() {

        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "disconnected to remoteAddress=" + socketAddressSupplier);
        tcpSocketConsumer.onConnectionClosed();

        if (clientConnectionMonitor != null) {
            @Nullable final SocketAddress socketAddress = socketAddressSupplier.get();
            if (socketAddress != null)
                clientConnectionMonitor.onDisconnected(name, socketAddress);
        }
    }

    private void onConnected() {

        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "connected to remoteAddress=" + socketAddressSupplier);

        if (clientConnectionMonitor != null) {
            @Nullable final SocketAddress socketAddress = socketAddressSupplier.get();
            if (socketAddress != null)
                clientConnectionMonitor.onConnected(name, socketAddress);
        }
    }

    /**
     * sets up subscriptions with the server, even if the socket connection is down, the
     * subscriptions will be re-establish with the server automatically once it comes back up. To
     * end the subscription with the server call {@code net.openhft.chronicle.network.connection.TcpChannelHub#unsubscribe(long)}
     *
     * @param asyncSubscription detail of the subscription that you wish to hold with the server
     */
    public void subscribe(@NotNull final AsyncSubscription asyncSubscription) {
        subscribe(asyncSubscription, false);
    }

    private void subscribe(@NotNull final AsyncSubscription asyncSubscription, boolean tryLock) {
        tcpSocketConsumer.subscribe(asyncSubscription, tryLock);
    }

    /**
     * closes a subscription established by {@code net.openhft.chronicle.network.connection.TcpChannelHub#
     * subscribe(net.openhft.chronicle.network.connection.AsyncSubscription)}
     *
     * @param tid the unique id of this subscription
     */
    public void unsubscribe(final long tid) {
        tcpSocketConsumer.unsubscribe(tid);
    }

    @NotNull
    public ReentrantLock outBytesLock() {
        return outBytesLock;
    }

    void doHandShaking(@NotNull SocketChannel socketChannel) throws IOException {

        assert outBytesLock.isHeldByCurrentThread();
        @Nullable final SessionDetails sessionDetails = sessionDetails();
        if (sessionDetails != null) {
            handShakingWire.clear();
            @NotNull final Bytes<?> bytes = handShakingWire.bytes();
            bytes.clear();

            // we are always going to send the header as text wire, the server will
            // respond in the wire define by the wireType field, all subsequent types must be in wireType

            handShakingWire.writeDocument(false, wireOut -> {
                wireOut.writeEventName(EventId.userId).text(sessionDetails.userId());
                wireOut.writeEventName(EventId.domain).text(sessionDetails.domain());
                wireOut.writeEventName(EventId.sessionMode).text(sessionDetails.sessionMode().toString());
                wireOut.writeEventName(EventId.securityToken).text(sessionDetails.securityToken());
                wireOut.writeEventName(EventId.clientId).text(sessionDetails.clientId().toString());
                wireOut.writeEventName(EventId.wireType).text(wireType.toString());
            });

            writeSocket1(handShakingWire, socketChannel);
        }
    }

    @Nullable
    private SessionDetails sessionDetails() {
        if (sessionProvider == null)
            return null;
        return sessionProvider.get();
    }

    /**
     * closes the existing connections
     */
    synchronized void closeSocket() {

        @Nullable SocketChannel clientChannel = this.clientChannel;

        if (clientChannel != null) {

            try {
                clientChannel.socket().shutdownInput();
            } catch (ClosedChannelException ignored) {

            } catch (IOException e) {
                Jvm.debug().on(getClass(), e);
            }

            try {
                clientChannel.socket().shutdownOutput();
            } catch (ClosedChannelException ignored) {

            } catch (IOException e) {
                Jvm.debug().on(getClass(), e);
            }

            Closeable.closeQuietly(clientChannel);

            this.clientChannel = null;

            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "closing", new StackTrace("only added for logging - please ignore !"));

            @NotNull final TcpSocketConsumer tcpSocketConsumer = this.tcpSocketConsumer;

            tcpSocketConsumer.tid = 0;
            tcpSocketConsumer.omap.clear();

            onDisconnected();

        }
    }

    public boolean isOpen() {
        return clientChannel != null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void notifyClosing() {
        // close early if possible.
        close();
    }

    /**
     * called when we are completed finished with using the TcpChannelHub
     */
    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        tcpSocketConsumer.prepareToShutdown();

        if (shouldSendCloseMessage)

            eventLoop.addHandler(new EventHandler() {
                @Override
                public boolean action() throws InvalidEventHandlerException {
                    try {
                        TcpChannelHub.this.sendCloseMessage();
                        tcpSocketConsumer.stop();
                        closed = true;

                        if (LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), "closing connection to " + socketAddressSupplier);

                        while (clientChannel != null) {

                            if (LOG.isDebugEnabled())
                                Jvm.debug().on(getClass(), "waiting for disconnect to " + socketAddressSupplier);
                        }
                    } catch (ConnectionDroppedException e) {
                        throw new InvalidEventHandlerException(e);
                    }

                    // we just want this to run once
                    throw new InvalidEventHandlerException();
                }

                @NotNull
                @Override
                public String toString() {
                    return TcpChannelHub.class.getSimpleName() + "..close()";
                }
            });
    }

    /**
     * used to signal to the server that the client is going to drop the connection, and waits up to
     * one second for the server to acknowledge the receipt of this message
     */
    private void sendCloseMessage() {

        this.lock(() -> {

            TcpChannelHub.this.writeMetaDataForKnownTID(0, outWire, null, 0);

            TcpChannelHub.this.outWire.writeDocument(false, w ->
                    w.writeEventName(EventId.onClientClosing).text(""));

        }, TryLock.LOCK);

        // wait up to 1 seconds to receive an close request acknowledgment from the server
        try {
            final boolean await = receivedClosedAcknowledgement.await(1, TimeUnit.SECONDS);
            if (!await)
                if (Jvm.isDebugEnabled(getClass()))
                    Jvm.debug().on(getClass(), "SERVER IGNORED CLOSE REQUEST: shutting down the client anyway as the " +
                            "server did not respond to the close() request.");
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param timeMs in milliseconds
     * @return a unique transactionId
     */
    public long nextUniqueTransaction(long timeMs) {
        long id = timeMs;
        for (; ; ) {
            long old = transactionID.get();
            if (old >= id)
                id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    /**
     * sends data to the server via TCP/IP
     *
     * @param wire the {@code wire} containing the outbound data
     */
    public void writeSocket(@NotNull final WireOut wire, boolean reconnectOnFailure) {
        assert outBytesLock().isHeldByCurrentThread();

        try {
            assert wire.startUse();
            @Nullable SocketChannel clientChannel = this.clientChannel;

            // wait for the channel to be non null
            if (clientChannel == null) {
                if (!reconnectOnFailure) {
                    return;
                }
                final byte[] bytes = wire.bytes().toByteArray();
                assert wire.endUse();
                condition.await(10, TimeUnit.SECONDS);
                assert wire.startUse();
                wire.clear();
                wire.bytes().write(bytes);
            }

            writeSocket1(wire, this.clientChannel);
        } catch (ClosedChannelException e) {
            closeSocket();
            Jvm.pause(500);
            if (reconnectOnFailure)
                throw new ConnectionDroppedException(e);

        } catch (IOException e) {
            if (!"Broken pipe".equals(e.getMessage()))
                Jvm.warn().on(getClass(), e);
            closeSocket();
            Jvm.pause(500);
            throw new ConnectionDroppedException(e);

        } catch (ConnectionDroppedException e) {
            closeSocket();
            Jvm.pause(500);
            throw e;

        } catch (Exception e) {
            Jvm.warn().on(getClass(), e);
            closeSocket();
            Jvm.pause(500);
            throw new ConnectionDroppedException(e);

        } finally {
            assert wire.endUse();
        }
    }

    /**
     * blocks for a message with the appropriate {@code tid}
     *
     * @param timeoutTime the amount of time to wait ( in MS ) before a time out exceptions
     * @param tid         the {@code tid} of the message that we are waiting for
     * @return the wire of the message with the {@code tid}
     */
    public Wire proxyReply(long timeoutTime, final long tid) throws ConnectionDroppedException, TimeoutException {

        try {
            return tcpSocketConsumer.syncBlockingReadSocket(timeoutTime, tid);

        } catch (ConnectionDroppedException e) {
            closeSocket();
            throw e;

        } catch (Throwable e) {
            Jvm.warn().on(getClass(), e);
            closeSocket();
            throw e;
        }
    }

    /**
     * writes the bytes to the socket, onto the clientChannel provided
     *
     * @param outWire the data that you wish to write
     */
    private void writeSocket1(@NotNull WireOut outWire, @Nullable SocketChannel clientChannel) throws
            IOException {

        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "sending :" + Wires.fromSizePrefixedBlobs((Wire) outWire));

        if (clientChannel == null) {
            LOG.info("Connection Dropped");
            throw new ConnectionDroppedException("Connection Dropped");
        }

        assert outBytesLock.isHeldByCurrentThread();

        long start = Time.currentTimeMillis();
        assert outWire.startUse();
        try {
            @NotNull final Bytes<?> bytes = outWire.bytes();
            @Nullable final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
            outBuffer.limit((int) bytes.writePosition());
            outBuffer.position(0);

            // this check ensure that a put does not occur while currently re-subscribing
            assert outBytesLock().isHeldByCurrentThread();

            boolean isOutBufferFull = false;
            logToStandardOutMessageSent(outWire, outBuffer);
            updateLargestChunkSoFarSize(outBuffer);

            try {

                int prevRemaining = outBuffer.remaining();
                while (outBuffer.remaining() > 0) {

                    // if the socket was changed, we need to resend using this one instead
                    // unless the client channel still has not be set, then we will use this one
                    // this can happen during the handshaking phase of a new connection

                    if (clientChannel != this.clientChannel)
                        throw new ConnectionDroppedException("Connection has Changed");

                    int len = clientChannel.write(outBuffer);
                    if (len == -1)
                        throw new IORuntimeException("Disconnection to server=" +
                                socketAddressSupplier + ", name=" + name);

                    if (LOG.isDebugEnabled())
                        Jvm.debug().on(getClass(), "W:" + len + ",socket=" + socketAddressSupplier.get());

                    // reset the timer if we wrote something.
                    if (prevRemaining != outBuffer.remaining()) {
                        start = Time.currentTimeMillis();
                        isOutBufferFull = false;
                        //  if (Jvm.isDebug() && outBuffer.remaining() == 0)
                        //    System.out.println("W: " + (prevRemaining - outBuffer
                        //          .remaining()));
                        prevRemaining = outBuffer.remaining();
                        @NotNull final TcpSocketConsumer tcpSocketConsumer = this.tcpSocketConsumer;

                        if (tcpSocketConsumer != null)
                            this.tcpSocketConsumer.lastTimeMessageReceivedOrSent = start;
                    } else {
                        if (!isOutBufferFull && Jvm.isDebug() && LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), "----> TCP write buffer is FULL! " + outBuffer.remaining() + " bytes" +
                                    " remaining.");
                        isOutBufferFull = true;

                        long writeTime = Time.currentTimeMillis() - start;

                        // the reason that this is so large is that results from a bootstrap can
                        // take a very long time to send all the data from the server to the client
                        // we don't want this to fail as it will cause a disconnection !
                        if (writeTime > TimeUnit.MINUTES.toMillis(15)) {

                            for (@NotNull Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                                Thread thread = entry.getKey();
                                if (thread.getThreadGroup().getName().equals("system"))
                                    continue;
                                @NotNull StringBuilder sb = new StringBuilder();
                                sb.append("\n========= THREAD DUMP =========\n");
                                sb.append(thread).append(" ").append(thread.getState());
                                Jvm.trimStackTrace(sb, entry.getValue());
                                sb.append("\n");
                                Jvm.warn().on(getClass(), sb.toString());
                            }

                            closeSocket();

                            throw new IORuntimeException("Took " + writeTime + " ms " +
                                    "to perform a write, remaining= " + outBuffer.remaining());
                        }

                        // its important to yield, if the read buffer gets full
                        // we wont be able to write, lets give some time to the read thread !
                        Thread.yield();
                    }
                }
            } catch (IOException e) {
                closeSocket();
                throw e;
            }

            outBuffer.clear();
            bytes.clear();
        } finally {
            assert outWire.endUse();
        }
    }

    private void logToStandardOutMessageSent(@NotNull WireOut wire, @NotNull ByteBuffer outBuffer) {
        if (!YamlLogging.showClientWrites())
            return;

        @NotNull Bytes<?> bytes = wire.bytes();

        try {

            if (bytes.readRemaining() > 0)
                LOG.info(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                        .title + "\n" : "") + "" +
                        YamlLogging.writeMessage() + (YamlLogging.writeMessage().isEmpty() ?
                        "" : "\n\n") +
                        "sends:\n\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(bytes) +
                        "```");
            YamlLogging.title = "";
            YamlLogging.writeMessage("");

        } catch (Exception e) {
            Jvm.warn().on(getClass(), Bytes.toString(bytes), e);
        }
    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer the outbound buffer
     */
    private void updateLargestChunkSoFarSize(@NotNull ByteBuffer outBuffer) {
        int sizeOfThisChunk = (int) (outBuffer.limit() - limitOfLast);
        if (largestChunkSoFar < sizeOfThisChunk)
            largestChunkSoFar = sizeOfThisChunk;

        limitOfLast = outBuffer.limit();
    }

    public Wire outWire() {
        assert outBytesLock().isHeldByCurrentThread();
        return outWire;
    }

    public boolean isOutBytesLocked() {
        return outBytesLock.isLocked();
    }

    private void reflectServerHeartbeatMessage(@NotNull ValueIn valueIn) {

        if (!outBytesLock().tryLock()) {
            if (Jvm.isDebug() && LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "skipped sending back heartbeat, because lock is held !" +
                        outBytesLock);
            return;
        }

        try {

            // time stamp sent from the server, this is so that the server can calculate the round
            // trip time
            long timestamp = valueIn.int64();
            TcpChannelHub.this.writeMetaDataForKnownTID(0, outWire, null, 0);
            TcpChannelHub.this.outWire.writeDocument(false, w ->
                    // send back the time stamp that was sent from the server
                    w.writeEventName(EventId.heartbeatReply).int64(timestamp));
            writeSocket(outWire(), false);

        } finally {
            outBytesLock().unlock();
            assert !outBytesLock.isHeldByCurrentThread();
        }
    }

    public long writeMetaDataStartTime(long startTime, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        long tid = nextUniqueTransaction(startTime);
        writeMetaDataForKnownTID(tid, wire, csp, cid);
        return tid;
    }

    public void writeMetaDataForKnownTID(long tid, @NotNull Wire wire, @Nullable String csp,
                                         long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.writeEventName(CoreFields.csp).text(csp);
            else
                wireOut.writeEventName(CoreFields.cid).int64(cid);
            wireOut.writeEventName(CoreFields.tid).int64(tid);
        });
    }

    /**
     * The writes the meta data to wire - the async version does not contain the tid
     *
     * @param wire the wire that we will write to
     * @param csp  provide either the csp or the cid
     * @param cid  provide either the csp or the cid
     */
    public void writeAsyncHeader(@NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();

        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.writeEventName(CoreFields.csp).text(csp);
            else
                wireOut.writeEventName(CoreFields.cid).int64(cid);
        });
    }

    public boolean lock(@NotNull Task r) {
        return lock(r, TryLock.LOCK);
    }

    private boolean lock(@NotNull Task r, @NotNull TryLock tryLock) {
        return lock2(r, false, tryLock);
    }

    public boolean lock2(@NotNull Task r, boolean reconnectOnFailure, @NotNull TryLock tryLock) {
        assert !outBytesLock.isHeldByCurrentThread();
        try {
            if (clientChannel == null && !reconnectOnFailure)
                return TryLock.LOCK != tryLock;

            @NotNull final ReentrantLock lock = outBytesLock();
            if (TryLock.LOCK == tryLock) {
                try {
                    //   if (lock.isLocked())
                    //     LOG.info("Lock for thread=" + Thread.currentThread() + " was held by " +
                    // lock);
                    lock.lock();

                } catch (Throwable e) {
                    lock.unlock();
                    throw e;
                }
            } else {
                if (!lock.tryLock()) {
                    if (tryLock.equals(TryLock.TRY_LOCK_WARN))
                        if (Jvm.isDebugEnabled(getClass()))
                            Jvm.debug().on(getClass(), "FAILED TO OBTAIN LOCK thread=" + Thread.currentThread() + " on " +
                                    lock, new IllegalStateException());
                    return false;
                }
            }

            try {
                if (clientChannel == null && reconnectOnFailure)
                    checkConnection();

                r.run();

                assert checkWritesOnReadThread(tcpSocketConsumer);

                writeSocket(outWire(), reconnectOnFailure);

            } catch (ConnectionDroppedException e) {
                if (Jvm.isDebug())
                    Jvm.debug().on(getClass(), e);
                throw e;

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
                throw e;

            } finally {
                lock.unlock();
            }
            return true;
        } finally {
            assert !outBytesLock.isHeldByCurrentThread();
        }
    }

    /**
     * blocks until there is a connection
     */
    public void checkConnection() {
        long start = Time.currentTimeMillis();

        while (clientChannel == null) {

            tcpSocketConsumer.checkNotShutdown();

            if (start + timeoutMs > Time.currentTimeMillis())
                try {
                    condition.await(1, TimeUnit.MILLISECONDS);

                } catch (InterruptedException e) {
                    throw new IORuntimeException("Interrupted");
                }
            else
                throw new IORuntimeException("Not connected to " + socketAddressSupplier);
        }

        if (clientChannel == null)
            throw new IORuntimeException("Not connected to " + socketAddressSupplier);
    }

    /**
     * you are unlikely to want to call this method in a production environment the purpose of this
     * method is to simulate a network outage
     */
    public void forceDisconnect() {
        Closeable.closeQuietly(clientChannel);
    }

    public boolean isOutBytesEmpty() {
        return outWire.bytes().readRemaining() == 0;
    }

    @FunctionalInterface
    public interface Task {
        void run();
    }

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    class TcpSocketConsumer implements EventHandler {
        @NotNull
        private final Map<Long, Object> map = new ConcurrentHashMap<>();
        private final Map<Long, Object> omap = new ConcurrentHashMap<>();
        @NotNull
        private final ExecutorService service;
        @NotNull
        private final ThreadLocal<Wire> syncInWireThreadLocal = withInitial(() -> {
            Wire wire = wireType.apply(elasticByteBuffer());
            assert wire.startUse();
            return wire;
        });
        long lastheartbeatSentTime = 0;
        volatile long start = Long.MAX_VALUE;
        private long tid;
        private Bytes serverHeartBeatHandler = Bytes.elasticByteBuffer();
        private volatile long lastTimeMessageReceivedOrSent = Time.currentTimeMillis();
        private volatile boolean isShutdown;
        @Nullable
        private volatile Throwable shutdownHere = null;
        private long failedConnectionCount;
        private volatile boolean prepareToShutdown;
        private Thread readThread;

        TcpSocketConsumer() {
            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "constructor remoteAddress=" + socketAddressSupplier);

            service = newCachedThreadPool(
                    new NamedThreadFactory("TcpChannelHub-Reads-" + socketAddressSupplier, true));
            start();
        }

        /**
         * re-establish all the subscriptions to the server, this method calls the {@code
         * net.openhft.chronicle.network.connection.AsyncSubscription#applySubscribe()} for each
         * subscription, this could should establish a subscription with the server.
         */
        private void onReconnect() {

            preventSubscribeUponReconnect.forEach(this::unsubscribe);
            map.values().forEach(v -> {
                if (v instanceof AsyncSubscription) {
                    if (!(v instanceof AsyncTemporarySubscription))
                        ((AsyncSubscription) v).applySubscribe();
                }
            });

        }

        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        void onConnectionClosed() {
            map.values().forEach(v -> {
                if (v instanceof Bytes)
                    synchronized (v) {
                        v.notifyAll();
                    }
                if (v instanceof AsyncSubscription) {
                    ((AsyncSubscription) v).onClose();
                } else if (v instanceof Bytes) {
                    synchronized (v) {
                        v.notifyAll();
                    }
                }
            });
        }

        @NotNull
        @Override
        public HandlerPriority priority() {
            return priority;
        }

        /**
         * blocks this thread until a response is received from the socket
         *
         * @param timeoutTimeMs the amount of time to wait before a time out exceptions
         * @param tid           the {@code tid} of the message that we are waiting for
         */
        Wire syncBlockingReadSocket(final long timeoutTimeMs, long tid)
                throws TimeoutException, ConnectionDroppedException {
            long start = Time.currentTimeMillis();

            final Wire wire = syncInWireThreadLocal.get();
            wire.clear();

            @NotNull Bytes<?> bytes = wire.bytes();
            ((ByteBuffer) bytes.underlyingObject()).clear();

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (bytes) {

                if (LOG.isDebugEnabled())
                    Jvm.debug().on(getClass(), "tid=" + tid + " of client request");

                bytes.clear();

                registerSubscribe(tid, bytes);

                long end = start + timeoutTimeMs;
                try {
                    do {
                        long delay = end - System.currentTimeMillis();
                        if (delay <= 0)
                            break;
                        bytes.wait(delay);

                        if (clientChannel == null)
                            throw new ConnectionDroppedException("Connection Closed : the connection to the " +
                                    "server has been dropped.");

                    } while (bytes.readLimit() == 0 && !isShutdown);

                } catch (InterruptedException ie) {
                    @NotNull TimeoutException te = new TimeoutException();
                    te.initCause(ie);
                    throw te;
                }
            }

            logToStandardOutMessageReceived(wire);

            if (Time.currentTimeMillis() - start >= timeoutTimeMs) {
                throw new TimeoutException("timeoutTimeMs=" + timeoutTimeMs);
            }

            return wire;

        }

        private void registerSubscribe(long tid, Object bytes) {
            // this check ensure that a put does not occur while currently re-subscribing
            outBytesLock().isHeldByCurrentThread();
            //    if (bytes instanceof AbstractAsyncSubscription && !(bytes instanceof
            //          AsyncTemporarySubscription))

            final Object prev = map.put(tid, bytes);
            assert prev == null;
        }

        void subscribe(@NotNull final AsyncSubscription asyncSubscription, boolean tryLock) {
            // we add a synchronize to ensure that the asyncSubscription is added before map before
            // the clientChannel is assigned
            synchronized (this) {
                if (clientChannel == null) {

                    // this check ensure that a put does not occur while currently re-subscribing
                    outBytesLock().isHeldByCurrentThread();

                    registerSubscribe(asyncSubscription.tid(), asyncSubscription);
                    if (LOG.isDebugEnabled())
                        Jvm.debug().on(getClass(), "deferred subscription tid=" + asyncSubscription.tid() + "," +
                                "asyncSubscription=" + asyncSubscription);

                    // not currently connected
                    return;
                }
            }

            // we have lock here to prevent a race with the resubscribe upon a reconnection
            @NotNull final ReentrantLock lock = outBytesLock();
            if (tryLock) {
                if (!lock.tryLock())
                    return;
            } else {
                try {
                    // do a quick lock so you can see if it could not get the lock the first time.
                    if (!lock.tryLock()) {
                        while (!lock.tryLock(1, SECONDS)) {
                            if (isShuttingdown())
                                throw new IllegalStateException("Shutting down");
                            LOG.info("Waiting for lock " + Jvm.lockWithStack(lock));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            try {
                registerSubscribe(asyncSubscription.tid(), asyncSubscription);

                asyncSubscription.applySubscribe();

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);

            } finally {
                lock.unlock();
            }
        }

        /**
         * unsubscribes the subscription based upon the {@code tid}
         *
         * @param tid the unique identifier for the subscription
         */
        public void unsubscribe(long tid) {
            map.remove(tid);
        }

        /**
         * uses a single read thread, to process messages to waiting threads based on their {@code
         * tid}
         */
        @NotNull
        private void start() {
            checkNotShutdown();

            assert shutdownHere == null;
            assert !isShutdown;
            service.submit(() -> {
                readThread = Thread.currentThread();
                try {
                    running();

                } catch (ConnectionDroppedException e) {
                    if (Jvm.isDebug() && !prepareToShutdown)
                        Jvm.debug().on(getClass(), e);

                } catch (Throwable e) {
                    if (!prepareToShutdown)
                        Jvm.warn().on(getClass(), e);
                }
            });

            service.submit(() -> {
                int count = 0;
                @Nullable String lastMsg = null;
                while (!isShuttingdown()) {
                    Jvm.pause(50);
                    if (count++ < 2000 / 50)
                        continue;

                    long delay = System.currentTimeMillis() - start;
                    if (delay >= 150) {
                        StringBuilder sb = new StringBuilder().append(readThread).append(" at ").append(delay).append(" ms");
                        Jvm.trimStackTrace(sb, readThread.getStackTrace());

                        @NotNull String msg = sb.toString();
                        if (!msg.contains("sun.nio.ch.SocketChannelImpl.read")) {
                            if (delay < 20) {
                                lastMsg = msg;
                            } else {
                                if (lastMsg != null)
                                    LOG.info(lastMsg);
                                LOG.info(msg);
                                lastMsg = null;
                            }
                        }
                    }
                }
            });
        }

        public void checkNotShutdown() {
            if (isShutdown)
                throw new IORuntimeException("Called after shutdown", shutdownHere);
        }

        private void running() {
            try {
                final Wire inWire = wireType.apply(elasticByteBuffer());
                assert inWire != null;
                assert inWire.startUse();

                while (!isShuttingdown()) {

                    checkConnectionState();

                    try {
                        // if we have processed all the bytes that we have read in
                        @NotNull final Bytes<?> bytes = inWire.bytes();

                        // the number bytes ( still required  ) to read the size
                        blockingRead(inWire, SIZE_OF_SIZE);

                        final int header = bytes.readVolatileInt(0);
                        final long messageSize = size(header);

                        // read the data
                        start = System.currentTimeMillis();
                        if (Wires.isData(header)) {
                            assert messageSize < Integer.MAX_VALUE;

                            final boolean clearTid = processData(tid, Wires.isReady(header), header,
                                    (int) messageSize, inWire);

                            long timeTaken = System.currentTimeMillis() - start;
                            start = Long.MAX_VALUE;
                            if (timeTaken > 20)
                                LOG.info("Processing data=" + timeTaken + "ms");

                            if (clearTid)
                                tid = -1;

                        } else {
                            // read  meta data - get the tid
                            blockingRead(inWire, messageSize);
                            logToStandardOutMessageReceived(inWire);
                            // ensure the tid is reset
                            this.tid = -1;
                            inWire.readDocument((WireIn w) -> this.tid = CoreFields.tid(w), null);
                        }

                    } catch (@NotNull Exception e) {
                        start = Long.MAX_VALUE;

                        if (Jvm.isDebug() && LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), e);

                        tid = -1;
                        if (isShuttingdown()) {
                            break;

                        } else {
                            String message = e.getMessage();
                            if (e instanceof ConnectionDroppedException) {
                                if (Jvm.isDebugEnabled(getClass()))
                                    Jvm.debug().on(getClass(), "reconnecting due to dropped connection " + ((message == null) ? "" : message));
                            } else if (e instanceof IOException && "Connection reset by peer".equals(message)) {
                                Jvm.warn().on(getClass(), "reconnecting due to \"Connection reset by peer\" " + message);
                            } else {
                                Jvm.warn().on(getClass(), "reconnecting due to unexpected exception", e);
                            }
                            closeSocket();

                            long pauseMs = connectionStrategy == null ? 500 : connectionStrategy.pauseMillisBeforeReconnect();
                            Jvm.pause(pauseMs);

                        }
                    } finally {
                        start = Long.MAX_VALUE;
                        clear(inWire);
                    }
                }

            } catch (Throwable e) {
                if (!isShuttingdown())
                    Jvm.warn().on(getClass(), e);
            } finally {
                closeSocket();
            }
        }

        boolean isShutdown() {
            return isShutdown;
        }

        boolean isShuttingdown() {
            return isShutdown || prepareToShutdown;
        }

        /**
         * @param header message size in header form
         * @return the true size of the message
         */
        private long size(int header) {
            final long messageSize = Wires.lengthOf(header);
            assert messageSize > 0 : "Invalid message size " + messageSize;
            assert messageSize < 1 << 30 : "Invalid message size " + messageSize;
            return messageSize;
        }

        /**
         * @param tid         the transaction id of the message
         * @param isReady     if true, this will be the last message for this tid
         * @param header      message size in header form
         * @param messageSize the sizeof the wire message
         * @param inWire      the location the data will be writen to
         * @return {@code true} if the tid should not be used again
         * @throws IOException
         */
        private boolean processData(final long tid,
                                    final boolean isReady,
                                    final int header,
                                    final int messageSize,
                                    @NotNull Wire inWire) throws IOException {
            assert tid != -1;
            boolean isLastMessageForThisTid = false;
            long startTime = 0;
            @Nullable Object o = null;

            // tid == 0 for system messages
            if (tid != 0) {

                @Nullable final SocketChannel c = clientChannel;

                // this can occur if we received a shutdown
                if (c == null)
                    return false;

                // this loop if to handle the rare case where we receive the tid before its been registered by this class
                for (; !isShuttingdown() && c.isOpen(); ) {

                    o = map.get(tid);

                    // we only remove the subscription so they are AsyncTemporarySubscription, as the AsyncSubscription
                    // can not be remove from the map as they are required when you resubscribe when we loose connectivity
                    if (o == null) {
                        o = omap.get(tid);
                        if (o != null) {
                            blockingRead(inWire, messageSize);

                            logToStandardOutMessageReceivedInERROR(inWire);
                            throw new AssertionError("Found tid=" + tid + " in the old map.");
                        }
                    } else {
                        if (isReady && (o instanceof Bytes || o instanceof
                                AsyncTemporarySubscription)) {
                            omap.put(tid, map.remove(tid));
                            isLastMessageForThisTid = true;
                        }
                        break;
                    }

                    // this can occur if the server returns the response before we have started to
                    // listen to it

                    if (startTime == 0)
                        startTime = Time.currentTimeMillis();
                    else
                        Jvm.pause(1);

                    if (Time.currentTimeMillis() - startTime > 3_000) {

                        blockingRead(inWire, messageSize);
                        logToStandardOutMessageReceived(inWire);

                        if (Jvm.isDebugEnabled(getClass()))
                            Jvm.debug().on(getClass(), "unable to respond to tid=" + tid + ", given that we have " +
                                    "received a message we a tid which is unknown, this can occur " +
                                    "sometime if " +
                                    "the subscription has just become unregistered ( an the server " +
                                    "has not yet processed the unregister event ) ");
                        return isLastMessageForThisTid;
                    }
                }

                // this can occur if we received a shutdown
                if (o == null)
                    return isLastMessageForThisTid;

            }

            // heartbeat message sent from the server
            if (tid == 0) {

                processServerSystemMessage(header, messageSize);
                return isLastMessageForThisTid;
            }

            // for async
            if (o instanceof AsyncSubscription) {

                blockingRead(inWire, messageSize);
                logToStandardOutMessageReceived(inWire);
                @NotNull AsyncSubscription asyncSubscription = (AsyncSubscription) o;

                try {
                    asyncSubscription.onConsumer(inWire);

                } catch (Exception e) {
                    if (LOG.isDebugEnabled())
                        Jvm.debug().on(getClass(), "Removing " + tid + " " + o, e);
                    omap.remove(tid);
                }
            }

            // for sync
            if (o instanceof Bytes) {
                @Nullable final Bytes bytes = (Bytes) o;
                // for sync
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (bytes) {
                    bytes.clear();
                    bytes.ensureCapacity(SIZE_OF_SIZE + messageSize);
                    @Nullable final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
                    byteBuffer.clear();
                    // we have to first write the header back to the bytes so that is can be
                    // viewed as a document
                    bytes.writeInt(0, header);
                    byteBuffer.position(SIZE_OF_SIZE);
                    byteBuffer.limit(SIZE_OF_SIZE + messageSize);
                    readBuffer(byteBuffer);
                    bytes.readLimit(byteBuffer.position());
                    bytes.notifyAll();
                }
            }
            return isLastMessageForThisTid;
        }

        /**
         * process system messages which originate from the server
         *
         * @param header      a value representing the type of message
         * @param messageSize the size of the message
         * @throws IOException
         */
        private void processServerSystemMessage(final int header, final int messageSize)
                throws IOException {

            serverHeartBeatHandler.clear();
            final Bytes bytes = serverHeartBeatHandler;

            bytes.clear();
            @NotNull final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
            byteBuffer.clear();
            // we have to first write the header back to the bytes so that is can be
            // viewed as a document
            bytes.writeInt(0, header);
            byteBuffer.position(SIZE_OF_SIZE);
            byteBuffer.limit(SIZE_OF_SIZE + messageSize);
            readBuffer(byteBuffer);

            bytes.readLimit(byteBuffer.position());

            final StringBuilder eventName = Wires.acquireStringBuilder();
            final Wire inWire = TcpChannelHub.this.wireType.apply(bytes);
            if (YamlLogging.showHeartBeats())
                logToStandardOutMessageReceived(inWire);
            inWire.readDocument(null, d -> {
                        @NotNull final ValueIn valueIn = d.readEventName(eventName);
                        if (EventId.heartbeat.contentEquals(eventName))
                            reflectServerHeartbeatMessage(valueIn);
                        else if (EventId.onClosingReply.contentEquals(eventName))
                            receivedClosedAcknowledgement.countDown();

                    }
            );
        }

        /**
         * blocks indefinitely until the number of expected bytes is received
         *
         * @param wire          the wire that the data will be written into, this wire must contain
         *                      an underlying ByteBuffer
         * @param numberOfBytes the size of the data to read
         * @throws IOException if anything bad happens to the socket connection
         */
        private void blockingRead(@NotNull final WireIn wire, final long numberOfBytes)
                throws IOException {

            @NotNull final Bytes<?> bytes = wire.bytes();
            bytes.ensureCapacity(bytes.writePosition() + numberOfBytes);

            @NotNull final ByteBuffer buffer = (ByteBuffer) bytes.underlyingObject();
            final int start = (int) bytes.writePosition();
            //noinspection ConstantConditions
            buffer.position(start);

            buffer.limit((int) (start + numberOfBytes));
            readBuffer(buffer);
            bytes.readLimit(buffer.position());
        }

        private void readBuffer(@NotNull final ByteBuffer buffer) throws IOException {

            //  long start = System.currentTimeMillis();
            boolean emptyRead = true;
            while (buffer.remaining() > 0) {
                @Nullable final SocketChannel clientChannel = TcpChannelHub.this.clientChannel;
                if (clientChannel == null)
                    throw new IOException("Disconnection to server=" + socketAddressSupplier +
                            " channel is closed, name=" + name);
                int numberOfBytesRead = clientChannel.read(buffer);

                WanSimulator.dataRead(numberOfBytesRead);
                if (numberOfBytesRead == -1)
                    throw new ConnectionDroppedException("Disconnection to server=" + socketAddressSupplier +
                            " read=-1 "
                            + ", name=" + name);

                if (numberOfBytesRead > 0) {
                    onMessageReceived();
                    emptyRead = false;

                    if (LOG.isDebugEnabled())
                        Jvm.debug().on(getClass(), "R:" + numberOfBytesRead + ",socket=" + socketAddressSupplier.get());
                    pauser.reset();

                } else if (numberOfBytesRead == 0 && isOpen()) {
                    // if we have not received a message from the server after the HEATBEAT_TIMEOUT_PERIOD
                    // we will drop and then re-establish the connection.
                    long millisecondsSinceLastMessageReceived = System.currentTimeMillis() - lastTimeMessageReceivedOrSent;
                    if (millisecondsSinceLastMessageReceived - HEATBEAT_TIMEOUT_PERIOD > 0) {
                        throw new IOException("reconnecting due to heartbeat failure, time since " +
                                "last message=" + millisecondsSinceLastMessageReceived + "ms " +
                                "dropping connection to " + socketAddressSupplier);
                    }
                    if (emptyRead)
                        start = Long.MAX_VALUE;
                    pauser.pause();
                    if (start == Long.MAX_VALUE)
                        start = System.currentTimeMillis();

                } else {
                    throw new ConnectionDroppedException(name + " is shutdown, was connected to "
                            + socketAddressSupplier);
                }

                if (isShutdown)
                    throw new ConnectionDroppedException(name + " is shutdown, was connected to " +
                            "" + socketAddressSupplier);

                if (lastTimeMessageReceivedOrSent + 60_000 < System.currentTimeMillis()) {

                    for (@NotNull Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                        Thread thread = entry.getKey();
                        if (thread == null ||
                                thread.getThreadGroup() == null ||
                                thread.getThreadGroup().getName() == null ||
                                thread.getThreadGroup().getName().equals("system"))
                            continue;
                        @NotNull StringBuilder sb = new StringBuilder();
                        sb.append(thread).append(" ").append(thread.getState());
                        Jvm.trimStackTrace(sb, entry.getValue());
                        sb.append("\n");
                        Jvm.warn().on(getClass(), "\n========= THREAD DUMP =========\n" + sb);
                    }

                    throw new ConnectionDroppedException(name + " the client is failing to get the" +
                            " " +
                            "data from the server, so we are going to drop the connection and " +
                            "reconnect.");
                }
            }
        }

        private void onMessageReceived() {
            lastTimeMessageReceivedOrSent = Time.currentTimeMillis();
        }

        /**
         * sends a heartbeat from the client to the server and logs the round trip time
         */
        private void sendHeartbeat() {
            TcpChannelHub.this.lock(this::sendHeartbeat0, TryLock.TRY_LOCK_IGNORE);
        }

        private void sendHeartbeat0() {
            assert outWire.startUse();
            try {
                if (outWire.bytes().writePosition() > 100)
                    return;

                long l = System.nanoTime();

                // this denotes that the next message is a system message as it has a null csp

                subscribe(new AbstractAsyncTemporarySubscription(TcpChannelHub.this, null, name) {
                    @Override
                    public void onSubscribe(@NotNull WireOut wireOut) {
                        if (Jvm.isDebug())
                            LOG.info("sending heartbeat");
                        wireOut.writeEventName(EventId.heartbeat).int64(Time
                                .currentTimeMillis());
                    }

                    @Override
                    public void onConsumer(@NotNull WireIn inWire) {
                        long roundTipTimeMicros = NANOSECONDS.toMicros(System.nanoTime() - l);
                        if (LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), "heartbeat round trip time=" + roundTipTimeMicros + "" +
                                    " server=" + socketAddressSupplier);

                        inWire.clear();
                    }
                }, true);
            } finally {
                assert outWire.endUse();
            }
        }

        /**
         * called when we are completed finished with using the TcpChannelHub, after this method is
         * called you will no longer be able to use this instance to received or send data
         */
        void stop() {

            if (isShutdown)
                return;

            if (shutdownHere == null)
                shutdownHere = new StackTrace(Thread.currentThread() + " Shutdown here");

            isShutdown = true;
            service.shutdown();
            try {
                service.awaitTermination(100, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

            } finally {
                service.shutdownNow();
            }
        }

        /**
         * gets called periodically to monitor the heartbeat
         *
         * @return true, if processing was performed
         * @throws InvalidEventHandlerException
         */
        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (clientChannel == null)
                throw new InvalidEventHandlerException();

            // a heartbeat only gets sent out if we have not received any data in the last
            // HEATBEAT_PING_PERIOD milliseconds
            long currentTime = Time.currentTimeMillis();
            long millisecondsSinceLastMessageReceived = currentTime - lastTimeMessageReceivedOrSent;
            long millisecondsSinceLastHeatbeatSend = currentTime - lastheartbeatSentTime;

            if (millisecondsSinceLastMessageReceived >= HEATBEAT_PING_PERIOD &&
                    millisecondsSinceLastHeatbeatSend >= HEATBEAT_PING_PERIOD) {
                lastheartbeatSentTime = Time.currentTimeMillis();
                sendHeartbeat();
            }

            return true;
        }

        private void checkConnectionState() {
            if (clientChannel != null)
                return;

            attemptConnect();
        }

        private void attemptConnect() {

            keepSubscriptionsAndClearEverythingElse();
            long start = System.currentTimeMillis();
            socketAddressSupplier.resetToPrimary();

            for (int i = 0; ; i++) {
                checkNotShutdown();

                if (LOG.isDebugEnabled())
                    Jvm.debug().on(getClass(), "attemptConnect remoteAddress=" + socketAddressSupplier);
                else if (i >= socketAddressSupplier.all().size() && !isShuttingdown())
                    LOG.info("attemptConnect remoteAddress=" + socketAddressSupplier);

                @Nullable SocketChannel socketChannel = null;
                try {

                    if (isShuttingdown())
                        continue;

                    socketChannel = connectionStrategy.connect(name, socketAddressSupplier, false, clientConnectionMonitor);

                    if (isShuttingdown())
                        continue;

                    if (socketChannel == null) {
                        Jvm.pause(1_000);
                        continue;
                    }

                    // this lock prevents the clients attempt to send data before we have
                    // finished the handshaking

                    if (!outBytesLock().tryLock(20, TimeUnit.SECONDS))
                        throw new IORuntimeException("failed to obtain the outBytesLock " + outBytesLock);

                    try {

                        clear(outWire);

                        // resets the heartbeat timer
                        onMessageReceived();

                        synchronized (this) {
                            LOG.info("connected to " + socketChannel);
                            clientChannel = socketChannel;
                        }

                        // the hand-shaking is assigned before setting the clientChannel, so that it can
                        // be assured to go first
                        doHandShaking(socketChannel);

                        eventLoop.addHandler(this);
                        if (LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), "successfully connected to remoteAddress=" +
                                    socketAddressSupplier);
                        onReconnect();

                        condition.signalAll();
                        onConnected();
                    } finally {
                        outBytesLock().unlock();
                        assert !outBytesLock.isHeldByCurrentThread();
                    }

                    return;
                } catch (Exception e) {
                    if (isShutdown || prepareToShutdown) {
                        closeSocket();
                        throw new IORuntimeException("shutting down");
                    } else {
                        Jvm.warn().on(getClass(), "failed to connect remoteAddress=" + socketAddressSupplier
                                + " so will reconnect ", e);
                        closeSocket();
                    }

                    Jvm.pause(1_000);
                }
            }
        }

        private void keepSubscriptionsAndClearEverythingElse() {

            tid = 0;
            omap.clear();

            @NotNull final Set<Long> keys = new HashSet<>(map.keySet());

            keys.forEach(k -> {
                final Object o = map.get(k);
                if (o instanceof Bytes || o instanceof AsyncTemporarySubscription)
                    map.remove(k);
            });
        }

        void prepareToShutdown() {
            this.prepareToShutdown = true;
            try {
                service.awaitTermination(100, MILLISECONDS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            service.shutdown();
        }
    }
}
