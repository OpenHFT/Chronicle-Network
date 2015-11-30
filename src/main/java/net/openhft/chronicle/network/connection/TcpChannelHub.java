/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ConnectionDroppedException;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.WanSimulator;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.lang.Integer.getInteger;
import static java.lang.ThreadLocal.withInitial;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.core.Jvm.pause;
import static net.openhft.chronicle.core.Jvm.rethrow;

/**
 * The TcpChannelHub is used to send your messages to the server and then read the servers response.
 * The TcpChannelHub ensures that each response is marshalled back onto the appropriate client
 * thread. It does this through the use of a unique transaction ID ( we call this transaction ID the
 * "tid" ), when the server responds to the client, its expected that the server sends back the tid
 * as the very first field in the message. The TcpChannelHub will look at each message and read the
 * tid, and then marshall the message onto your appropriate client thread. Created by Rob Austin
 */
public class TcpChannelHub implements Closeable {

    public static final int HEATBEAT_PING_PERIOD =
            !Jvm.IS_DEBUG ? getInteger("heartbeat.ping.period", 5000) :
                    getInteger("heartbeat.ping.period", 5000);
    public static final int HEATBEAT_TIMEOUT_PERIOD =
            !Jvm.IS_DEBUG ? getInteger("heartbeat.timeout", 15_000) :
                    getInteger("heartbeat.timeout", 15_000);

    public static final int SIZE_OF_SIZE = 4;
    public static final Set<TcpChannelHub> hubs = new CopyOnWriteArraySet<>();
    private static final Logger LOG = LoggerFactory.getLogger(TcpChannelHub.class);
    public final long timeoutMs;
    @NotNull
    protected final String name;
    protected final int tcpBufferSize;
    final Wire outWire;
    final Wire inWire;
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
    private final Function<Bytes, Wire> wire;
    private final Wire handShakingWire;
    private final ClientConnectionMonitor clientConnectionMonitor;
    // private final String description;
    private long largestChunkSoFar = 0;
    @Nullable
    private volatile SocketChannel clientChannel;
    private volatile boolean closed;
    private CountDownLatch receivedClosedAcknowledgement = new CountDownLatch(1);
    // set up in the header
    private long limitOfLast = 0;
    private boolean shouldSendCloseMessage;
    private HandlerPriority priority;


    public TcpChannelHub(@Nullable final SessionProvider sessionProvider,
                         @NotNull final EventLoop eventLoop,
                         @NotNull final Function<Bytes, Wire> wire,
                         @NotNull final String name,
                         @NotNull final SocketAddressSupplier socketAddressSupplier,
                         boolean shouldSendCloseMessage,
                         @Nullable ClientConnectionMonitor clientConnectionMonitor,
                         @NotNull final HandlerPriority monitor) {
        this.priority = monitor;
        this.socketAddressSupplier = socketAddressSupplier;
        this.eventLoop = eventLoop;
        this.tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", 2 << 20);
        this.outWire = wire.apply(elasticByteBuffer());
        this.inWire = wire.apply(elasticByteBuffer());
        this.name = name;
        this.timeoutMs = Integer.getInteger("tcp.client.timeout", 10_000);
        this.wire = wire;
        this.handShakingWire = wire.apply(Bytes.elasticByteBuffer());
        this.sessionProvider = sessionProvider;
        this.tcpSocketConsumer = new TcpSocketConsumer(wire);
        this.shouldSendCloseMessage = shouldSendCloseMessage;
        this.clientConnectionMonitor = clientConnectionMonitor;
        hubs.add(this);
    }

    public static void assertAllHubsClosed() {
        StringBuilder errors = new StringBuilder();
        for (TcpChannelHub h : hubs) {
            if (!h.isClosed())
                errors.append("Connection ").append(h).append(" still open\n");
            h.close();
        }
        hubs.clear();
        if (errors.length() > 0)
            throw new AssertionError(errors.toString());
    }

    public static void closeAllHubs() {
        final TcpChannelHub[] tcpChannelHub = hubs.toArray(new TcpChannelHub[hubs.size()]);
        for (int i = tcpChannelHub.length - 1; i >= 0; i--) {

            final TcpChannelHub tcpChannelHub1 = tcpChannelHub[i];
            if (!tcpChannelHub1.isClosed()) {
                LOG.warn("Closing " + tcpChannelHub1);
                tcpChannelHub1.close();
            }
        }
        hubs.clear();
    }

    static void logToStandardOutMessageReceived(@NotNull Wire wire) {
        final Bytes<?> bytes = wire.bytes();

        if (!YamlLogging.clientReads)
            return;

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {
            try {

                LOG.info("\nreceives:\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(bytes) +
                        "```\n");
                YamlLogging.title = "";
                YamlLogging.writeMessage = "";
            } catch (Exception e) {

                LOG.error(Bytes.toString(bytes), e);
            }
        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    static void logToStandardOutMessageReceivedInERROR(@NotNull Wire wire) {
        final Bytes<?> bytes = wire.bytes();

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {
            try {

                LOG.info("\nreceives IN ERROR:\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(bytes) +
                        "```\n");
                YamlLogging.title = "";
                YamlLogging.writeMessage = "";
            } catch (Exception e) {

                String x = Bytes.toString(bytes);
                LOG.error(x, e);
            }
        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    private void clear(@NotNull final Wire wire) {
        wire.clear();
        ((ByteBuffer) wire.bytes().underlyingObject()).clear();
    }

    @Nullable
    SocketChannel openSocketChannel(InetSocketAddress socketAddress) throws IOException {
        final SocketChannel result = SocketChannel.open(socketAddress);
        result.configureBlocking(false);
        Socket socket = result.socket();
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        socket.setSoTimeout(0);
        socket.setSoLinger(false, 0);

        return result;
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
            LOG.debug("disconnected to remoteAddress=" + socketAddressSupplier);
        tcpSocketConsumer.onConnectionClosed();

        if (clientConnectionMonitor != null) {
            final SocketAddress socketAddress = socketAddressSupplier.get();
            if (socketAddress != null)
                clientConnectionMonitor.onDisconnected(name, socketAddress);
        }

    }

    private void onConnected() {

        if (LOG.isDebugEnabled())
            LOG.debug("connected to remoteAddress=" + socketAddressSupplier);

        if (clientConnectionMonitor != null) {
            final SocketAddress socketAddress = socketAddressSupplier.get();
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

    public void subscribe(@NotNull final AsyncSubscription asyncSubscription, boolean tryLock) {
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
        final SessionDetails sessionDetails = sessionDetails();
        if (sessionDetails != null) {
            handShakingWire.clear();
            handShakingWire.bytes().clear();
            handShakingWire.writeDocument(false, wireOut -> {
                wireOut.writeEventName(EventId.userId).text(sessionDetails.userId());
                wireOut.writeEventName(EventId.domain).text(sessionDetails.domain());
                wireOut.writeEventName(EventId.sessionMode).text(sessionDetails.sessionMode().toString());
                wireOut.writeEventName(EventId.securityToken).text(sessionDetails.securityToken());
                wireOut.writeEventName(EventId.clientId).text(sessionDetails.clientId().toString());
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
    protected synchronized void closeSocket() {

        SocketChannel clientChannel = this.clientChannel;

        if (clientChannel != null) {

            try {
                clientChannel.socket().shutdownInput();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.socket().shutdownOutput();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.socket().close();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.close();
            } catch (IOException ignored) {
            }

            this.clientChannel = null;

            clear(inWire);
            lock(() -> clear(outWire));

            final TcpSocketConsumer tcpSocketConsumer = this.tcpSocketConsumer;

            tcpSocketConsumer.tid = 0;
            tcpSocketConsumer.omap.clear();

            onDisconnected();
        }
    }

    public boolean isOpen() {
        return clientChannel != null;
    }

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
    public void close() {

        if (closed)
            return;

        tcpSocketConsumer.prepareToShutdown();

        if (shouldSendCloseMessage)

            eventLoop.addHandler(() ->
            {
                try {
                    sendCloseMessage();
                    tcpSocketConsumer.stop();
                    closed = true;

                    if (LOG.isDebugEnabled())
                        LOG.debug("closing connection to " + socketAddressSupplier);

                    while (clientChannel != null) {

                        if (LOG.isDebugEnabled())
                            LOG.debug("waiting for disconnect to " + socketAddressSupplier);
                    }
                } catch (ConnectionDroppedException ignore) {


                }

                // we just want this to run once
                throw new InvalidEventHandlerException();

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


        }, false);

        // wait up to 1 seconds to receive an close request acknowledgment from the server
        try {
            final boolean await = receivedClosedAcknowledgement.await(1, TimeUnit.SECONDS);
            if (!await)
                LOG.debug("SERVER IGNORED CLOSE REQUEST: shutting down the client anyway as the " +
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

            SocketChannel clientChannel = this.clientChannel;

            // wait for the channel to be non null
            if (clientChannel == null) {
                if (reconnectOnFailure)
                    condition.await(10, TimeUnit.SECONDS);
                else
                    return;
            }


            writeSocket1(wire, clientChannel);
        } catch (ClosedChannelException e) {
            closeSocket();
            if (reconnectOnFailure)
                throw new ConnectionDroppedException(e);
        } catch (IOException e) {
            if (!"Broken pipe".equals(e.getMessage()))
                LOG.error("", e);
            closeSocket();
            throw new ConnectionDroppedException(e);

        } catch (Exception e) {
            LOG.error("", e);
            closeSocket();
            throw new ConnectionDroppedException(e);
        }
    }

    /**
     * blocks for a message with the appreciate {@code tid}
     *
     * @param timeoutTime the amount of time to wait ( in MS ) before a time out exceptions
     * @param tid         the {@code tid} of the message that we are waiting for
     * @return the wire of the message with the {@code tid}
     * @throws ConnectionDroppedException
     */
    public Wire proxyReply(long timeoutTime, final long tid) throws ConnectionDroppedException {

        try {
            return tcpSocketConsumer.syncBlockingReadSocket(timeoutTime, tid);
        } catch (ConnectionDroppedException e) {
            closeSocket();
            throw e;
        } catch (@NotNull AssertionError | RuntimeException e) {
            LOG.error("", e);
            closeSocket();
            throw e;
        } catch (Exception e) {
            LOG.error("", e);
            closeSocket();
            throw rethrow(e);
        }
    }

    /**
     * writes the bytes to the socket, onto the clientChannel provided
     *
     * @param outWire       the data that you wish to write
     * @param clientChannel
     * @throws IOException
     */
    private void writeSocket1(@NotNull WireOut outWire, @Nullable SocketChannel clientChannel) throws
            IOException {

        if (clientChannel == null)
            throw new ConnectionDroppedException("Connection Dropped");

        assert outBytesLock.isHeldByCurrentThread();

        long start = Time.currentTimeMillis();
        for (; ; ) {


            final Bytes<?> bytes = outWire.bytes();
            final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
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
                    SocketChannel clientChannel1 = this.clientChannel;


                    if (clientChannel != clientChannel1)
                        throw new ConnectionDroppedException("Connection has Changed");

                    int len = clientChannel.write(outBuffer);

                    if (len == -1)
                        throw new IORuntimeException("Disconnection to server=" +
                                socketAddressSupplier + ", name=" + name);

                    // reset the timer if we wrote something.
                    if (prevRemaining != outBuffer.remaining()) {
                        start = Time.currentTimeMillis();
                        isOutBufferFull = false;
                        if (Jvm.isDebug() && outBuffer.remaining() == 0)
                            System.out.println("W: " + (prevRemaining - outBuffer
                                    .remaining()));
                        prevRemaining = outBuffer.remaining();
                    } else {
                        if (!isOutBufferFull && Jvm.isDebug())
                            System.out.println("-------------> Out Buffer is FULL!");
                        isOutBufferFull = true;

                        long writeTime = Time.currentTimeMillis() - start;

                        // the reason that this is so large is that results from a bootstrap can
                        // take a very long time to send all the data from the server to the client
                        // we don't want this to fail as it will cause a disconnection !
                        if (writeTime > TimeUnit.MINUTES.toMillis(15)) {

                            for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                                Thread thread = entry.getKey();
                                if (thread.getThreadGroup().getName().equals("system"))
                                    continue;
                                StringBuilder sb = new StringBuilder();
                                sb.append(thread).append(" ").append(thread.getState());
                                Jvm.trimStackTrace(sb, entry.getValue());
                                sb.append("\n");
                                LOG.error("\n========= THREAD DUMP =========\n", sb);
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
            return;
        }
    }

    private void logToStandardOutMessageSent(@NotNull WireOut wire, @NotNull ByteBuffer outBuffer) {
        if (!YamlLogging.clientWrites)
            return;

        Bytes<?> bytes = wire.bytes();

        try {

            if (bytes.readRemaining() > 0)
                LOG.info(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                        .title + "\n" : "") + "" +
                        YamlLogging.writeMessage + (YamlLogging.writeMessage.isEmpty() ?
                        "" : "\n\n") +
                        "sends:\n\n" +
                        "```yaml\n" +
                        Wires.fromSizePrefixedBlobs(bytes) +
                        "```");
            YamlLogging.title = "";
            YamlLogging.writeMessage = "";
        } catch (Exception e) {
            LOG.error(Bytes.toString(bytes), e);
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

    public boolean outWireIsEmpty() {
        return outWire.bytes().writePosition() == 0;
    }


    void reflectServerHeartbeatMessage(@NotNull ValueIn valueIn) {

        if (!outBytesLock().tryLock()) {
            System.out.println("skipped sending back heartbeat, because lock is held !" +
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
        return lock(r, false);
    }

    public boolean lock(@NotNull Task r, boolean tryLock) {
        return lock2(r, false, tryLock);
    }

    public boolean lock2(@NotNull Task r, boolean reconnectOnFailure, boolean tryLock) {
        assert !outBytesLock.isHeldByCurrentThread();
        try {
            if (clientChannel == null && !reconnectOnFailure)
                return tryLock;

            final ReentrantLock lock = outBytesLock();
            if (tryLock) {
                if (!lock.tryLock()) {
                    LOG.warn("FAILED TO OBTAIN LOCK thread=" + Thread.currentThread() + " on " + lock);
                    return false;
                }
            } else try {
                if (lock.isLocked())
                    LOG.info("Lock for thread=" + Thread.currentThread() + " was held by " + lock);
                lock.lock();

            } catch (Throwable e) {
                lock.unlock();
                throw e;
            }

            try {
                if (clientChannel == null && reconnectOnFailure)
                    checkConnection();

                r.run();
                assert Thread.currentThread() != tcpSocketConsumer.readThread : "if writes and reads are on the same thread this can lead " +
                        "to deadlocks with the server, if the server buffer becomes full";
                writeSocket(outWire(), reconnectOnFailure);

            } catch (ConnectionDroppedException e) {
                throw Jvm.rethrow(e);
            } catch (Exception e) {
                LOG.error("", e);
                throw Jvm.rethrow(e);
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
                    condition.await(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            else
                throw new IORuntimeException("Not connected to " + socketAddressSupplier);
        }
    }

    /**
     * you are unlikely to want to call this method in a production environment the purpose of this
     * method is to simulate a network outage
     */
    public void forceDisconnect() {
        try {
            final SocketChannel clientChannel = this.clientChannel;
            if (clientChannel != null)
                clientChannel.close();
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    public interface Task {
        void run();
    }

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    private class TcpSocketConsumer implements EventHandler {
        @NotNull
        private final ExecutorService executorService;

        @NotNull
        private final Map<Long, Object> map = new ConcurrentHashMap<>();
        private final Map<Long, Object> omap = new ConcurrentHashMap<>();
        long lastheartbeatSentTime = 0;
        private Function<Bytes, Wire> wireFunction;
        private long tid;
        @NotNull
        private ThreadLocal<Wire> syncInWireThreadLocal = withInitial(() -> wire.apply(
                elasticByteBuffer()));
        private Bytes serverHeartBeatHandler = Bytes.elasticByteBuffer();

        private volatile long lastTimeMessageReceived = Time.currentTimeMillis();
        private volatile boolean isShutdown;
        @Nullable
        private volatile Throwable shutdownHere = null;

        private long failedConnectionCount;
        private volatile boolean prepareToShutdown;
        private Thread readThread;

        /**
         * @param wireFunction converts bytes into wire, ie TextWire or BinaryWire
         */
        private TcpSocketConsumer(
                @NotNull final Function<Bytes, Wire> wireFunction) {
            this.wireFunction = wireFunction;
            if (LOG.isDebugEnabled())
                LOG.debug("constructor remoteAddress=" + socketAddressSupplier);

            executorService = start();
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
        public void onConnectionClosed() {
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
         * @throws InterruptedException
         */
        Wire syncBlockingReadSocket(final long timeoutTimeMs, long tid) throws
                InterruptedException, TimeoutException, ConnectionDroppedException {
            long start = Time.currentTimeMillis();

            final Wire wire = syncInWireThreadLocal.get();
            wire.clear();

            Bytes<?> bytes = wire.bytes();
            ((ByteBuffer) bytes.underlyingObject()).clear();

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (bytes) {

                if (LOG.isDebugEnabled())
                    LOG.debug("tid=" + tid + " of client request");

                bytes.clear();

                registerSubscribe(tid, bytes);

                do {
                    bytes.wait(timeoutTimeMs);

                    if (clientChannel == null)
                        throw new ConnectionDroppedException("Connection Closed : the connection to the " +
                                "server has been dropped.");

                } while (bytes.readLimit() == 0 && !isShutdown);
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
                        LOG.debug("deferred subscription tid=" + asyncSubscription.tid() + "," +
                                "asyncSubscription=" + asyncSubscription);

                    // not currently connected
                    return;
                }
            }

            // we have lock here to prevent a race with the resubscribe upon a reconnection
            final ReentrantLock reentrantLock = outBytesLock();
            if (tryLock) {
                if (!reentrantLock.tryLock())
                    return;
            } else {
                reentrantLock.lock();
            }
            try {

                registerSubscribe(asyncSubscription.tid(), asyncSubscription);

                asyncSubscription.applySubscribe();
            } catch (Exception e) {
                LOG.error("", e);
            } finally {
                reentrantLock.unlock();
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
        private ExecutorService start() {
            checkNotShutdown();

            final ExecutorService executorService = newSingleThreadExecutor(
                    new NamedThreadFactory("TcpChannelHub-Reads-" + socketAddressSupplier, true));
            assert shutdownHere == null;
            assert !isShutdown;
            executorService.submit(() -> {
                readThread = Thread.currentThread();
                try {
                    running();
                } catch (ConnectionDroppedException e) {
                    if (!tcpSocketConsumer.prepareToShutdown)
                        LOG.info(e.toString());
                } catch (IORuntimeException e) {
                    if (!prepareToShutdown)
                        LOG.error("", e);
                } catch (Throwable e) {
                    if (!prepareToShutdown)
                        LOG.error("", e);
                }
            });

            return executorService;
        }

        public void checkNotShutdown() {
            if (isShutdown)
                throw new IORuntimeException("Called after shutdown", shutdownHere);
        }

        private void running() {
            try {
                final Wire inWire = wireFunction.apply(elasticByteBuffer());
                assert inWire != null;

                while (!isShuttingdown()) {

                    checkConnectionState();

                    try {
                        // if we have processed all the bytes that we have read in
                        final Bytes<?> bytes = inWire.bytes();

                        // the number bytes ( still required  ) to read the size
                        blockingRead(inWire, SIZE_OF_SIZE);

                        final int header = bytes.readVolatileInt(0);
                        final long messageSize = size(header);

                        // read the data
                        if (Wires.isData(header)) {
                            assert messageSize < Integer.MAX_VALUE;
                            final boolean clearTid = processData(tid, Wires.isReady(header), header,
                                    (int) messageSize, inWire);
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

                        tid = -1;
                        if (isShuttingdown()) {
                            break;
                        } else {
                            String message = e.getMessage();
                            if (e instanceof IOException && "Connection reset by peer".equals(message))
                                LOG.warn("reconnecting due to \"Connection reset by peer\" " + message);

                            if (e instanceof ConnectionDroppedException)
                                LOG.debug("reconnecting due to dropped connection " + ((message == null) ? "" : message));
                            else
                                LOG.warn("reconnecting due to unexpected exception", e);
                            closeSocket();
                            Thread.sleep(500);
                        }
                    } finally {
                        clear(inWire);
                    }
                }

            } catch (Throwable e) {
                if (!isShuttingdown())
                    LOG.error("", e);
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
                                    @NotNull Wire inWire) throws IOException, InterruptedException {
            assert tid != -1;
            boolean isLastMessageForThisTid = false;
            long startTime = 0;
            Object o = null;

            // tid == 0 for system messages
            if (tid != 0) {

                final SocketChannel c = clientChannel;

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
                        Thread.sleep(1);

                    if (Time.currentTimeMillis() - startTime > 3_000) {

                        blockingRead(inWire, messageSize);
                        logToStandardOutMessageReceived(inWire);

                        LOG.debug("unable to respond to tid=" + tid + ", given that we have " +
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
                AsyncSubscription asyncSubscription = (AsyncSubscription) o;

                asyncSubscription.onConsumer(inWire);

            }

            // for sync
            if (o instanceof Bytes) {
                final Bytes bytes = (Bytes) o;
                // for sync
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (bytes) {
                    bytes.clear();
                    bytes.ensureCapacity(SIZE_OF_SIZE + messageSize);
                    final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
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
            final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
            byteBuffer.clear();
            // we have to first write the header back to the bytes so that is can be
            // viewed as a document
            bytes.writeInt(0, header);
            byteBuffer.position(SIZE_OF_SIZE);
            byteBuffer.limit(SIZE_OF_SIZE + messageSize);
            readBuffer(byteBuffer);

            bytes.readLimit(byteBuffer.position());

            final StringBuilder eventName = Wires.acquireStringBuilder();
            final Wire inWire = wire.apply(bytes);
            if (YamlLogging.showHeartBeats)
                logToStandardOutMessageReceived(inWire);
            inWire.readDocument(null, d -> {
                        final ValueIn valueIn = d.readEventName(eventName);
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

            final Bytes<?> bytes = wire.bytes();
            bytes.ensureCapacity(bytes.writePosition() + numberOfBytes);

            final ByteBuffer buffer = (ByteBuffer) bytes.underlyingObject();
            final int start = (int) bytes.writePosition();
            buffer.position(start);

            buffer.limit((int) (start + numberOfBytes));
            readBuffer(buffer);
            bytes.readLimit(buffer.position());
        }

        private void readBuffer(@NotNull final ByteBuffer buffer) throws IOException {

            long start = System.currentTimeMillis();
            while (buffer.remaining() > 0) {
                final SocketChannel clientChannel = TcpChannelHub.this.clientChannel;
                if (clientChannel == null)
                    throw new IOException("Disconnection to server=" + socketAddressSupplier +
                            " channel is closed, name=" + name);
                int numberOfBytesRead = clientChannel.read(buffer);
                if (Jvm.isDebug() && numberOfBytesRead > 0)
                    System.out.println("R: " + numberOfBytesRead + " plus " +
                            buffer.remaining());

                WanSimulator.dataRead(numberOfBytesRead);
                if (numberOfBytesRead == -1)
                    throw new ConnectionDroppedException("Disconnection to server=" + socketAddressSupplier +
                            " read=-1 "
                            + ", name=" + name);

                if (numberOfBytesRead > 0) {
                    onMessageReceived();
                    start = System.currentTimeMillis();

                } else if (numberOfBytesRead == 0 && isOpen()) {
                    // if we have not received a message from the server after the HEATBEAT_TIMEOUT_PERIOD
                    // we will drop and then re-establish the connection.
                    long millisecondsSinceLastMessageReceived = System.currentTimeMillis() - lastTimeMessageReceived;
                    if (millisecondsSinceLastMessageReceived - HEATBEAT_TIMEOUT_PERIOD > 0) {
                        throw new IOException("reconnecting due to heartbeat failure, time since " +
                                "last message=" + millisecondsSinceLastMessageReceived + "ms " +
                                "dropping connection to " + socketAddressSupplier);
                    }
                } else
                    throw new ConnectionDroppedException(name + " is shutdown, was connected to "
                            + socketAddressSupplier);

                if (isShutdown)
                    throw new ConnectionDroppedException(name + " is shutdown, was connected to " +
                            "" + socketAddressSupplier);

                if (start + 30_000 < System.currentTimeMillis()) {

                    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                        Thread thread = entry.getKey();
                        if (thread.getThreadGroup().getName().equals("system"))
                            continue;
                        StringBuilder sb = new StringBuilder();
                        sb.append(thread).append(" ").append(thread.getState());
                        Jvm.trimStackTrace(sb, entry.getValue());
                        sb.append("\n");
                        LOG.error("\n========= THREAD DUMP =========\n" + sb);
                    }

                    LOG.error("", new ConnectionDroppedException(name + " the client is failing to get the data from the server"));
                }
            }
        }

        private void onMessageReceived() {
            lastTimeMessageReceived = Time.currentTimeMillis();
        }

        /**
         * sends a heartbeat from the client to the server and logs the round trip time
         */
        private void sendHeartbeat() {

            if (outWire.bytes().writePosition() > 100)
                return;

            long l = System.nanoTime();

            // this denotes that the next message is a system message as it has a null csp

            subscribe(new AbstractAsyncTemporarySubscription(TcpChannelHub.this, null, name) {
                @Override
                public void onSubscribe(@NotNull WireOut wireOut) {
                    System.out.println("sending heartbeat");
                    lock(() -> wireOut.writeEventName(EventId.heartbeat).int64(Time
                            .currentTimeMillis()));
                }

                @Override
                public void onConsumer(@NotNull WireIn inWire) {
                    long roundTipTimeMicros = NANOSECONDS.toMicros(System.nanoTime() - l);
                    if (LOG.isDebugEnabled())
                        LOG.debug("heartbeat round trip time=" + roundTipTimeMicros + "" +
                                " server=" + socketAddressSupplier);

                    inWire.clear();
                }
            }, true);
        }

        /**
         * called when we are completed finished with using the TcpChannelHub, after this method is
         * called you will no longer be able to use this instance to received or send data
         */
        private void stop() {

            if (isShutdown)
                return;

            if (shutdownHere == null)
                shutdownHere = new Throwable(Thread.currentThread() + " Shutdown here");

            isShutdown = true;
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS))
                    executorService.shutdownNow();
            } catch (InterruptedException e) {
                executorService.shutdownNow();
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
            long millisecondsSinceLastMessageReceived = currentTime - lastTimeMessageReceived;
            long millisecondsSinceLastHeatbeatSend = currentTime - lastheartbeatSentTime;

            if (millisecondsSinceLastMessageReceived >= HEATBEAT_PING_PERIOD &&
                    millisecondsSinceLastHeatbeatSend >= HEATBEAT_PING_PERIOD) {
                lastheartbeatSentTime = Time.currentTimeMillis();
                sendHeartbeat();
            }

            return true;
        }

        private void checkConnectionState() throws IOException {
            if (clientChannel != null)
                return;

            attemptConnect();
        }

        private void attemptConnect() throws IOException {

            keepSubscriptionsAndClearEverythingElse();
            long start = System.currentTimeMillis();
            socketAddressSupplier.startAtFirstAddress();

            OUTER:
            for (int i = 1; ; i++) {
                checkNotShutdown();

                if (LOG.isDebugEnabled())
                    LOG.debug("attemptConnect remoteAddress=" + socketAddressSupplier);
                SocketChannel socketChannel = null;
                try {

                    INNER_LOOP:
                    for (; ; ) {

                        if (isShuttingdown())
                            continue OUTER;


                        if (start + socketAddressSupplier.timeoutMS() < System.currentTimeMillis()) {

                            String oldAddress = socketAddressSupplier.toString();

                            socketAddressSupplier.failoverToNextAddress();
                            if ("(none)".equals(oldAddress)) {
                                LOG.info("Connection Dropped to address=" +
                                        oldAddress + ", so will fail over to" +
                                        socketAddressSupplier + ", name=" + name);
                            }

                            if (socketAddressSupplier.get() == null) {
                                LOG.warn("failed to establish a socket " +
                                        "connection of any of the following servers=" +
                                        socketAddressSupplier.all() + " so will re-attempt");
                                socketAddressSupplier.startAtFirstAddress();
                            }

                            // reset the timer, so that we can try this new address for a while
                            start = System.currentTimeMillis();
                        }


                        try {

                            final InetSocketAddress socketAddress = socketAddressSupplier.get();
                            if (socketAddress == null) {
                                socketAddressSupplier.failoverToNextAddress();
                                continue;
                            }

                            socketChannel = openSocketChannel(socketAddress);

                            if (socketChannel == null) {
                                LOG.error("Unable to open socketChannel to remoteAddress=" +
                                        socketAddressSupplier);
                                pause(1_000);
                                continue;
                            }

                            // success
                            this.failedConnectionCount = 0;
                            break;

                        } catch (Throwable e) {

                            if (prepareToShutdown) {
                                throw e;
                            }
                            socketAddressSupplier.failoverToNextAddress();

                            //  logs with progressive back off, to prevent the log files from
                            // being filled up
                            if ((failedConnectionCount < 15 && failedConnectionCount % 5 == 0) ||
                                    failedConnectionCount % 60 == 0)
                                LOG.info("Server is unavailable, failed to  connect to" +
                                        "address=" + socketAddressSupplier);

                            failedConnectionCount++;

                            Closeable.closeQuietly(socketChannel);
                            pause(1_000);
                        }
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
                            LOG.debug("successfully connected to remoteAddress=" +
                                    socketAddressSupplier);
                        onReconnect();
                        onConnected();
                        condition.signalAll();
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
                        LOG.error("failed to connect remoteAddress=" + socketAddressSupplier
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

            final HashSet keys = new HashSet(map.keySet());

            keys.forEach(k -> {

                final Object o = map.get(k);
                if (o instanceof Bytes || o instanceof AsyncTemporarySubscription)
                    map.remove(k);

            });
        }

        public void prepareToShutdown() {
            this.prepareToShutdown = true;
        }
    }
}
