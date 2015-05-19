/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.client;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.RemoteCallTimeoutException;
import net.openhft.chronicle.hash.impl.util.CloseablesManager;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by Rob Austin
 */
public class ClientWiredStatelessTcpConnectionHub {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWiredStatelessTcpConnectionHub.class);

    public static final int SIZE_OF_SIZE = 4;

    protected final String name;
    protected final InetSocketAddress remoteAddress;
    public final long timeoutMs;
    protected final int tcpBufferSize;
    private final ReentrantLock inBytesLock = new ReentrantLock(true);
    private final ReentrantLock outBytesLock = new ReentrantLock();

    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);

    @Nullable
    protected CloseablesManager closeables;

    final Wire outWire = new TextWire(Bytes.elasticByteBuffer());
    long largestChunkSoFar = 0;
    public final Wire inWire = new TextWire(Bytes.elasticByteBuffer());

    //  used by the enterprise version
    public int localIdentifier;


    private SocketChannel clientChannel;
    // this is a transaction id and size that has been read by another thread.
    private volatile long parkedTransactionId;

    private volatile long parkedTransactionTimeStamp;
    private long limitOfLast = 0;

    // set up in the header
    private long startTime;
    private boolean doHandShaking;

    public ClientWiredStatelessTcpConnectionHub(
            byte localIdentifier,
            boolean doHandShaking,
            InetSocketAddress remoteAddress,
            int tcpBufferSize,
            long timeout) {

        this.localIdentifier = localIdentifier;
        this.doHandShaking = doHandShaking;
        this.tcpBufferSize = tcpBufferSize;
        this.remoteAddress = remoteAddress;
        this.name = " connected to " + remoteAddress.toString();
        this.timeoutMs = timeout;

        attemptConnect(remoteAddress);
    }

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) {

        // ensures that the excising connection are closed
        closeExisting();

        try {
            SocketChannel socketChannel = openSocketChannel(closeables);

            if (socketChannel.connect(remoteAddress))
                clientChannel = socketChannel;

        } catch (IOException e) {
            LOG.error("", e);
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
        }
    }

    public ReentrantLock inBytesLock() {
        return inBytesLock;
    }

    public ReentrantLock outBytesLock() {
        return outBytesLock;
    }

    protected void checkVersion(@NotNull String csp) {

        final String serverVersion = serverApplicationVersion(csp);
        final String clientVersion = clientVersion();

        if (!serverVersion.equals(clientVersion)) {
            LOG.warn("DIFFERENT CHRONICLE-MAP VERSIONS: The Chronicle-Map-Server and " +
                    "Stateless-Client are on different " +
                    "versions, " +
                    " we suggest that you use the same version, server=" +
                    serverApplicationVersion(csp) + ", " +
                    "client=" + clientVersion);
        }
    }

    public static boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().
            getInputArguments().toString().indexOf("jdwp") >= 0;

    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis() && !IS_DEBUG)
            throw new RemoteCallTimeoutException("timeout=" + timeoutTime + "ms");
    }

    protected synchronized void lazyConnect(final long timeoutMs,
                                            final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        SocketChannel result;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = openSocketChannel(closeables);
                if (!result.connect(remoteAddress)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }

                result.socket().setTcpNoDelay(true);
                if (doHandShaking)

                    break;
            } catch (IOException e) {
                if (closeables != null) closeables.closeQuietly();
            } catch (Exception e) {
                if (closeables != null) closeables.closeQuietly();
                throw e;
            }
        }
        clientChannel = result;
    }


    static SocketChannel openSocketChannel(final CloseablesManager closeables) throws IOException {
        SocketChannel result = null;

        try {
            result = SocketChannel.open();
            result.socket().setTcpNoDelay(true);
        } finally {
            if (result != null)
                try {
                    closeables.add(result);
                } catch (IllegalStateException e) {
                    // already closed
                }
        }
        return result;
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();

        closeables = new CloseablesManager();
    }


    public synchronized void close() {

        if (closeables != null)
            closeables.closeQuietly();


        closeables = null;
        clientChannel = null;

    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    public long nextUniqueTransaction(long time) {
        long id = time;
        for (; ; ) {
            long old = transactionID.get();
            if (old >= id) id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    @NotNull
    public String serverApplicationVersion(@NotNull String csp) {
        TextWire wire = new TextWire(Bytes.elasticByteBuffer());
        String result = proxyReturnString(Events.getApplicationVersion, wire, csp, 0);
        return (result == null) ? "" : result;
    }

    @SuppressWarnings("WeakerAccess")
    @NotNull
    String clientVersion() {
        throw new UnsupportedOperationException("todo (clientVersion)");
    }


    /**
     * sends data to the server via TCP/IP
     *
     * @param wire the {@code wire} containing the outbound data
     */
    public void writeSocket(@NotNull final Wire wire) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();


        final long timeoutTime = startTime + this.timeoutMs;
        try {

            for (; ; ) {
                if (clientChannel == null)
                    lazyConnect(timeoutMs, remoteAddress);
                try {


                    // send out all the bytes
                    writeSocket(wire, timeoutTime);
                    break;

                } catch (java.nio.channels.ClosedChannelException e) {
                    checkTimeout(timeoutTime);
                    lazyConnect(timeoutMs, remoteAddress);
                }
            }
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private void writeLength(Wire outWire) {
        assert outBytesLock().isHeldByCurrentThread();
        final Bytes<?> bytes = outWire.bytes();
        long position = bytes.position();
        if (position > Integer.MAX_VALUE || position < Integer.MIN_VALUE)
            throw new IllegalStateException("message too large");

        long pos = bytes.position();
        try {
            bytes.reset();
            long size = position - bytes.position();
            bytes.writeUnsignedInt(size - SIZE_OF_SIZE);
        } finally {
            bytes.position(pos);
        }
    }

    public Wire proxyReply(long timeoutTime, final long tid) {

        assert inBytesLock().isHeldByCurrentThread();

        try {
            return proxyReplyThrowable(timeoutTime, tid);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            LOG.error("name=" + name, e);
            throw e;
        }
    }


    private Wire proxyReplyThrowable(long timeoutTime, long tid) throws IOException {
        for (; ; ) {

            assert inBytesLock().isHeldByCurrentThread();

            // if we have processed all the bytes that we have read in
            final Bytes<?> bytes = inWire.bytes();
            inWireClear();

            readSocket(SIZE_OF_SIZE, timeoutTime);

            final int header = bytes.readVolatileInt(bytes.position());
            final long messageSize = Wires.lengthOf(header);

            assert messageSize > 0 : "Invalid message size " + messageSize;
            assert messageSize < 1 << 30 : "Invalid message size " + messageSize;

            if (!Wires.isData(header)) {

                readSocket((int) messageSize, timeoutTime);

                inWire.readDocument((WireIn w) -> {
                    parkedTransactionId = w.read(CoreFields.tid).int64();

                    if (parkedTransactionId != tid) {

                        // if the transaction id is not for this thread, park it
                        // and allow another thread to pick it up
                        parkedTransactionTimeStamp = System.currentTimeMillis();
                        pause();

                    }

                }, null);
                continue;
            }

            if (parkedTransactionId == tid) {
                // the data is for this thread so process it
                readSocket((int) messageSize, timeoutTime);
                return inWire;
            } else if (System.currentTimeMillis() - timeoutTime > parkedTransactionTimeStamp)

                throw new IllegalStateException("Skipped Message with " +
                        "transaction-id=" +
                        parkedTransactionTimeStamp +
                        ", this can occur when you have another thread which has called the " +
                        "stateless client and terminated abruptly before the message has been " +
                        "returned from the server and hence consumed by the other thread.");
        }

    }

    /**
     * clears the wire and its underlying byte buffer
     */
    private void inWireClear() {
        inWireByteBuffer().clear();
        final Bytes<?> bytes = inWire.bytes();
        bytes.clear();
    }

    private void clearParked() {
        assert inBytesLock().isHeldByCurrentThread();
        parkedTransactionId = 0;
        parkedTransactionTimeStamp = 0;
    }

    private void pause() {

        assert !outBytesLock().isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        /// don't call inBytesLock.isHeldByCurrentThread() as it not atomic
        inBytesLock().unlock();

        // allows another thread to enter here
        inBytesLock().lock();
    }

    /**
     * reads up to the number of byte in {@code requiredNumberOfBytes} from the socket
     *
     * @param requiredNumberOfBytes the number of bytes to read
     * @param timeoutTime           timeout in milliseconds
     * @return bytes read from the TCP/IP socket
     * @throws java.io.IOException socket failed to read data
     */
    @SuppressWarnings("UnusedReturnValue")
    private void readSocket(int requiredNumberOfBytes, long timeoutTime) throws IOException {

        assert inBytesLock().isHeldByCurrentThread();

        ByteBuffer buffer = inWireByteBuffer();
        int position = buffer.position();

        try {
            buffer.limit(position + requiredNumberOfBytes);
        } catch (IllegalArgumentException e) {
            buffer = inWireByteBuffer(position + requiredNumberOfBytes);
            buffer.limit(position + requiredNumberOfBytes);
            buffer.position(position);
        }

        long start = buffer.position();

        while (buffer.position() - start < requiredNumberOfBytes) {
            assert clientChannel != null;

            if (clientChannel.read(buffer) == -1)
                throw new IORuntimeException("Disconnection to server");

            checkTimeout(timeoutTime);
        }

        final Bytes<?> bytes = inWire.bytes();
        bytes.limit(position + requiredNumberOfBytes);
    }

    private ByteBuffer inWireByteBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }

    private ByteBuffer inWireByteBuffer(long requiredCapacity) {
        final Bytes<?> bytes = inWire.bytes();
        bytes.ensureCapacity(requiredCapacity);
        return (ByteBuffer) bytes.underlyingObject();
    }

    /**
     * writes the bytes to the socket
     *
     * @param outWire     the data that you wish to write
     * @param timeoutTime how long before a we timeout
     * @throws java.io.IOException
     */
    private void writeSocket(Wire outWire, long timeoutTime) throws IOException {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        final Bytes<?> bytes = outWire.bytes();
        long outBytesPosition = bytes.position();

        // if we have other threads waiting to send and the buffer is not full,
        // let the other threads write to the buffer
        if (outBytesLock().hasQueuedThreads() &&
                outBytesPosition + largestChunkSoFar <= tcpBufferSize) {
            return;
        }

        final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
        outBuffer.limit((int) bytes.position());

        outBuffer.position(0);

        if (EventGroup.IS_DEBUG) {
            writeBytesToStandardOut(bytes, outBuffer);
        }

        upateLargestChunkSoFarSize(outBuffer);

        while (outBuffer.remaining() > 0) {

            int len = clientChannel.write(outBuffer);

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");

            // if we have queued threads then we don't have to write all the bytes as the other
            // threads will write the remains bytes.
            if (outBuffer.remaining() > 0 && outBytesLock().hasQueuedThreads() &&
                    outBuffer.remaining() + largestChunkSoFar <= tcpBufferSize) {

                if (LOG.isDebugEnabled())
                    LOG.debug("continuing -  without all the data being written to the buffer as " +
                            "it will be written by the next thread");
                outBuffer.compact();
                bytes.limit(outBuffer.limit());
                bytes.position(outBuffer.position());
                return;
            }

            checkTimeout(timeoutTime);

        }

        outBuffer.clear();
        bytes.clear();

    }

    private void writeBytesToStandardOut(Bytes<?> bytes, ByteBuffer outBuffer) {
        final long position = bytes.position();
        final long limit = bytes.limit();
        try {

            bytes.limit(outBuffer.limit());
            bytes.position(outBuffer.position());

            if (net.openhft.chronicle.wire.YamlLogging.clientWrites) {
                try {

                    System.out.println(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                            .title + "\n\n" : "") + "" +
                            YamlLogging.writeMessage + (YamlLogging.writeMessage.isEmpty() ?
                            "" : "\n\n") +
                            "sends:\n\n" +
                            "```yaml\n" +
                            Wires.fromSizePrefixedBlobs(bytes) +
                            "```");
                    net.openhft.chronicle.wire.YamlLogging.title = "";
                    net.openhft.chronicle.wire.YamlLogging.writeMessage = "";

                } catch (Exception e) {
                    System.out.println(
                            Bytes.toDebugString(bytes));
                }
            }

        } finally {
            bytes.limit(limit);
            bytes.position(position);
        }
    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer
     */
    private void upateLargestChunkSoFarSize(ByteBuffer outBuffer) {
        int sizeOfThisChunk = (int) (outBuffer.limit() - limitOfLast);
        if (largestChunkSoFar < sizeOfThisChunk)
            largestChunkSoFar = sizeOfThisChunk;

        limitOfLast = outBuffer.limit();
    }


    /**
     * @param eventName the event name
     * @param startTime the time the message was sent
     * @param wire
     * @param csp       the csp describing this nammed channel
     * @param cid       if the cid != 0 the cid will be used instead of the csp
     * @return the tid
     */
    private long proxySend(@NotNull final WireKey eventName,
                           final long startTime,
                           @NotNull final Wire wire,
                           @NotNull final String csp,
                           long cid) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        // send
        outBytesLock().lock();
        try {
            long tid = writeMetaData(startTime,wire, csp, cid);
            wire.writeDocument(false, wireOut -> {
                wireOut.writeEventName(eventName);
                wireOut.writeValue().marshallable(w -> {
                });
            });

            writeSocket(wire);
            return tid;
        } finally {
            outBytesLock().unlock();
        }
    }

 /*   @SuppressWarnings("SameParameterValue")
    @Nullable
    public String proxyReturnString(@NotNull final WireKey messageId, String csp, long cid) {
        return proxyReturnString(messageId, outWire, csp, cid);
    }*/

    @SuppressWarnings("SameParameterValue")
    @Nullable
    String proxyReturnString(@NotNull final WireKey eventId, Wire outWire, String csp, long cid) {
        final long startTime = System.currentTimeMillis();
        long tid;

        outBytesLock().lock();
        try {
            tid = proxySend(eventId, startTime, outWire, csp, cid);
        } finally {
            outBytesLock().unlock();
        }

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock().lock();
        try {
            final Wire wire = proxyReply(timeoutTime, tid);

            int datalen = wire.bytes().readVolatileInt();

            assert Wires.isData(datalen);

            return wire.read(CoreFields.reply).text();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            inBytesLock().unlock();
        }
    }


    public Wire outWire() {
        assert outBytesLock().isHeldByCurrentThread();
        return outWire;
    }


    public long writeMetaData(long startTime, Wire wire, String csp, long cid) {

        assert outBytesLock().isHeldByCurrentThread();
        startTime(startTime);
        long tid = nextUniqueTransaction(startTime);

        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.write(CoreFields.csp).text(csp);
            else
                wireOut.write(CoreFields.cid).int64(cid);
            wireOut.write(CoreFields.tid).int64(tid);
        });

        return tid;
    }


    /**
     * The writes the meta data to wire - the async version does not contain the tid
     * @param wire the wire that we will write to
     * @param csp provide either the csp or the cid
     * @param cid provide either the csp or the cid
     */
    public void writeAsyncHeader(Wire wire, String csp, long cid) {

        assert outBytesLock().isHeldByCurrentThread();

        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.write(CoreFields.csp).text(csp);
            else
                wireOut.write(CoreFields.cid).int64(cid);

        });

    }


    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    public enum Events implements WireKey {
        getApplicationVersion
    }

}
