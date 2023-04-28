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
package net.openhft.chronicle.network;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import net.openhft.chronicle.threads.MediumEventLoop;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.BitSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;
import static net.openhft.chronicle.network.internal.SocketExceptionUtil.isAConnectionResetException;

public class TcpEventHandler<T extends NetworkContext<T>>
        extends AbstractCloseable
        implements EventHandler, TcpEventHandlerManager<T> {

    public static final int TARGET_WRITE_SIZE = Jvm.getInteger("TcpEventHandler.targetWriteSize", 1024);
    /**
     * Maximum number of iterations we can go without performing idle work (to prevent starvation in busy handlers)
     */
    private static final int MAX_ITERATIONS_BETWEEN_IDLE_WORK = 100;
    @Deprecated(/* To be removed in x.24 */)
    private static final boolean CALL_MISSED_HEARTBEAT_ON_DISCONNECT;
    private static final int MONITOR_POLL_EVERY_SEC = Jvm.getInteger("tcp.event.monitor.secs", 10);
    private static final long NBR_WARNING_NANOS = Jvm.getLong("tcp.nbr.warning.nanos", 20_000_000L);
    private static final long NBW_WARNING_NANOS = Jvm.getLong("tcp.nbw.warning.nanos", 20_000_000L);
    private static final Logger LOG = LoggerFactory.getLogger(TcpEventHandler.class);
    private static final AtomicBoolean FIRST_HANDLER = new AtomicBoolean();
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1 << 30;
    public static boolean DISABLE_TCP_NODELAY = Jvm.getBoolean("disable.tcp_nodelay");

    private boolean flushedOut = false; // track output buffer empty state. interacts with NetworkContext onFlushed

    static {
        ThreadLogTypeElapsedRecord.loadClass();
        if (DISABLE_TCP_NODELAY) Jvm.startup().on(TcpEventHandler.class, "tcpNoDelay disabled");
        final String cmhodPropertyName = "chronicle.network.callOnMissedHeartbeatOnDisconnect";
        CALL_MISSED_HEARTBEAT_ON_DISCONNECT = Jvm.getBoolean(cmhodPropertyName);
        if (CALL_MISSED_HEARTBEAT_ON_DISCONNECT)
            Jvm.warn().on(TcpEventHandler.class, cmhodPropertyName + " is deprecated and will be removed in x.24");
    }

    private TcpEventHandler.SocketReader reader = new DefaultSocketReader();

    @NotNull
    private final ChronicleSocketChannel sc;
    private final String scToString;
    @NotNull
    private final T nc;
    @NotNull
    private final NetworkLog readLog;
    @NotNull
    private final NetworkLog writeLog;
    @NotNull
    private final Bytes<ByteBuffer> inBBB;
    @NotNull
    private final Bytes<ByteBuffer> outBBB;
    private final TcpHandlerBias.BiasController bias;

    private final boolean nbWarningEnabled;
    private final StatusMonitorEventHandler statusMonitorEventHandler;
    // prevent starvation of idle actions
    private int iterationsSinceIdle;

    @Nullable
    private volatile TcpHandler<T> tcpHandler;

    // allow for 20 seconds of slowness at startup
    private long lastTickReadTime;

    public TcpEventHandler(@NotNull final T nc) {
        this(nc, false);
    }

    public TcpEventHandler(@NotNull final T nc, final boolean fair) {
        this(nc, fair ? TcpHandlerBias.FAIR : TcpHandlerBias.READ);
    }

    public TcpEventHandler(@NotNull final T nc, final TcpHandlerBias bias) {
        this.sc = ChronicleSocketChannelFactory.wrapUnsafe(nc.socketChannel().socketChannel());
        this.scToString = sc.toString();
        this.nc = nc;
        this.bias = bias.get();
        lastTickReadTime = nc.timeProvider().currentTimeMillis() + 20_000;
        try {
            sc.configureBlocking(false);
            ChronicleSocket sock = sc.socket();
            // TODO: should have a strategy for this
            if (!DISABLE_TCP_NODELAY)
                sock.setTcpNoDelay(true);

            if (TCP_BUFFER >= 64 << 10) {
                sock.setReceiveBufferSize(TCP_BUFFER);
                sock.setSendBufferSize(TCP_BUFFER);

                checkBufSize(sock.getReceiveBufferSize(), "recv");
                checkBufSize(sock.getSendBufferSize(), "send");
            }
        } catch (IOException e) {
            if (isClosed() || !sc.isOpen())
                throw new IORuntimeException(e);
            Jvm.warn().on(getClass(), e);
        }

        //We have to provide back pressure to restrict the buffer growing beyond,2GB because it reverts to
        // being Native bytes, we should also provide back pressure if we are not able to keep up
        inBBB = Bytes.elasticByteBuffer(TCP_BUFFER + OS.pageSize(), max(TCP_BUFFER + OS.pageSize(), DEFAULT_MAX_MESSAGE_SIZE));
        outBBB = Bytes.elasticByteBuffer(TCP_BUFFER, max(TCP_BUFFER, DEFAULT_MAX_MESSAGE_SIZE));

        // must be set after we take a slice();
        outBBB.underlyingObject().limit(0);
        readLog = new NetworkLog(this.sc, "read");
        writeLog = new NetworkLog(this.sc, "write");
        nbWarningEnabled = Jvm.warn().isEnabled(getClass());
        statusMonitorEventHandler = new StatusMonitorEventHandler(getClass());
        singleThreadedCheckDisabled(true);

        if (FIRST_HANDLER.compareAndSet(false, true))
            warmUp();
    }

    @Override
    public void eventLoop(final EventLoop eventLoop) {
        if (eventLoop == null || eventLoop instanceof MediumEventLoop)
            return;
        try {
            eventLoop.addHandler(statusMonitorEventHandler);
        } catch (Exception e) {
            if (!eventLoop.isClosing())
                throw Jvm.rethrow(e);
        }
    }

    @Override
    public void singleThreadedCheckReset() {
        super.singleThreadedCheckReset();
        ((AbstractCloseable) nc).singleThreadedCheckReset();
    }

    public void reader(@NotNull final TcpEventHandler.SocketReader reader) {
        throwExceptionIfClosed();

        this.reader = reader;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        Jvm.safepoint();

        if (this.isClosing())
            throw new InvalidEventHandlerException();
        QueryCloseable c = sc;
        if (c.isClosing())
            throw new InvalidEventHandlerException();

        if (tcpHandler == null)
            return false;

        if (tcpHandler.hasTimedOut()) {
            closeAndStartReconnector();
            return false;
        }

        try {
            return action0();
        } catch (InvalidEventHandlerException t) {
            throw t;
        } catch (Throwable t) {
            if (this.isClosed())
                throw new InvalidEventHandlerException(t);
            throw Jvm.rethrow(t);
        }
    }

    private boolean action0() throws InvalidEventHandlerException {
        if (!sc.isOpen()) {
            tcpHandler.onEndOfConnection(false);
            closeQuietly(nc);
            throw new InvalidEventHandlerException("socket is closed");
        }

        statusMonitorEventHandler.incrementSocketPollCount();

        boolean busy = false;
        if (bias.canWrite())
            try {
                busy = writeAction();

            } catch (ClosedIllegalStateException cise) {
                Jvm.warn().on(getClass(), cise);
                throw new InvalidEventHandlerException();

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        if (bias.canRead())
            try {
                busy = readAction(busy);

            } catch (ClosedChannelException e) {
                closeAndStartReconnector();
                throw new InvalidEventHandlerException(e);
            } catch (IOException e) {
                handleIOE(e);
                throw new InvalidEventHandlerException();
            } catch (InvalidEventHandlerException e) {
                close();
                throw e;
            } catch (Exception e) {
                if (!isClosed()) {
                    close();
                    Jvm.warn().on(getClass(), "", e);
                }
                throw new InvalidEventHandlerException(e);
            }

        return busy;
    }

    private boolean readAction(boolean busy) throws IOException, InvalidEventHandlerException {
        final ByteBuffer inBB = inBBB.underlyingObject();
        final int start = inBB.position();

        assert !sc.isBlocking();
        final long beginNs = System.nanoTime();
        final int read = inBB.remaining() > 0 ? reader.read(sc, inBBB) : Integer.MAX_VALUE;
        //   int read = inBB.remaining() > 0 ? sc.read(inBB) : Integer.MAX_VALUE;
        final long elapsedNs = System.nanoTime() - beginNs;
        if (nbWarningEnabled && elapsedNs > NBR_WARNING_NANOS)
            statusMonitorEventHandler.add(
                    new ThreadLogTypeElapsedRecord(LogType.READ, elapsedNs, Affinity.getCpu(), Affinity.getAffinity()));

        if (read == Integer.MAX_VALUE)
            onInBBFul();
        if (read > 0) {
            tcpHandler.onReadTime(System.nanoTime(), inBB, start, inBB.position());
            lastTickReadTime = nc.timeProvider().currentTimeMillis();
            readLog.log(inBB, start, inBB.position());
            invokeHandler();
            busy = true;
        } else if (read == 0) {
            if (outBBB.readRemaining() > 0) {
                busy |= invokeHandler();
            }

            // check for timeout only here - in other branches we either just read something or are about to close socket anyway
            if (nc.heartbeatTimeoutMs() > 0) {
                final long tickTime = nc.timeProvider().currentTimeMillis();
                if (tickTime > lastTickReadTime + nc.heartbeatTimeoutMs()) {
                    final HeartbeatListener heartbeatListener = nc.heartbeatListener();
                    if (heartbeatListener != null && heartbeatListener.onMissedHeartbeat()) {
                        // implementer tries to recover - do not disconnect for some time
                        lastTickReadTime += heartbeatListener.lingerTimeBeforeDisconnect();
                    } else {
                        tcpHandler.onEndOfConnection(true);
                        closeAndStartReconnector();
                        throw new InvalidEventHandlerException("heartbeat timeout");
                    }
                }
            }
        } else {
            // read == -1, socketChannel has reached end-of-stream
            closeAndStartReconnector();

            throw new InvalidEventHandlerException("socket closed " + sc);
        }

        performIdleWorkIfDue(busy);

        return busy;
    }

    private void performIdleWorkIfDue(boolean busy) {
        if (!busy || iterationsSinceIdle > MAX_ITERATIONS_BETWEEN_IDLE_WORK) {
            if (tcpHandler != null) {
                tcpHandler.performIdleWork();
            }
            iterationsSinceIdle = 0;
        } else {
            iterationsSinceIdle++;
        }
    }

    /**
     * Closes the channel and triggers asynchronous reconnecting if it's a connector.
     */
    private void closeAndStartReconnector() {
        Jvm.debug().on(TcpEventHandler.class, "Closing and starting reconnector");
        close();
        if (!nc.isAcceptor()) {
            final Runnable socketReconnector = nc.socketReconnector();
            if (socketReconnector == null)
                Jvm.warn().on(getClass(), "socketReconnector == null");
            else
                socketReconnector.run();
        }
    }

    @Override
    public String toString() {
        return "TcpEventHandler{" +
                "sc=" + scToString + ", " +
                "tcpHandler=" + tcpHandler + ", " +
                "closed=" + isClosed() +
                '}';
    }

    public void warmUp() {
        Jvm.debug().on(TcpEventHandler.class, "Warming up...");
        final int runs = 12000;
        long beginNs = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            inBBB.readPositionRemaining(8, 1024);
            compactBuffer();
            clearBuffer();
        }
        long elapsedNs = System.nanoTime() - beginNs;
        Jvm.debug().on(TcpEventHandler.class, "... warmed up - took " + (elapsedNs / runs / 1e3) + " us avg");

        ((AbstractReferenceCounted) inBBB).singleThreadedCheckReset();
    }

    private void checkBufSize(final int bufSize, final String name) {
        if (bufSize < TCP_BUFFER) {
            LOG.warn("Attempted to set " + name + " tcp buffer to " + TCP_BUFFER + " but kernel only allowed " + bufSize);
        }
    }

    public ChronicleSocketChannel socketChannel() {
        return sc;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        final ServerThreadingStrategy sts = nc.serverThreadingStrategy();
        switch (sts) {
            case SINGLE_THREADED:
                return singleThreadedPriority();
            case CONCURRENT:
                return HandlerPriority.CONCURRENT;
            default:
                throw new UnsupportedOperationException("todo");
        }
    }

    @NotNull
    public HandlerPriority singleThreadedPriority() {
        return nc.priority();
    }

    @Nullable
    public TcpHandler<T> tcpHandler() {
        return tcpHandler;
    }

    @Override
    public void tcpHandler(final TcpHandler<T> tcpHandler) {
        throwExceptionIfClosedInSetter();

        nc.onHandlerChanged(tcpHandler);
        this.tcpHandler = tcpHandler;
    }

    @Override

    public void loopFinished() {
        // Release unless already released
        inBBB.releaseLast();
        outBBB.releaseLast();
    }

    public void onInBBFul() {
        LOG.trace("inBB is full, can't read from socketChannel");
    }

    @PackageLocal
    boolean invokeHandler() throws IOException {
        Jvm.safepoint();
        boolean busy = false;
        final int position = inBBB.underlyingObject().position();
        inBBB.readLimit(position);
        outBBB.writePosition(outBBB.underlyingObject().limit());

        long lastInBBBReadPosition;
        do {
            lastInBBBReadPosition = inBBB.readPosition();
            tcpHandler.process(inBBB, outBBB, nc);

            statusMonitorEventHandler.addBytesRead(inBBB.readPosition() - lastInBBBReadPosition);

            // process method might change the underlying ByteBuffer by resizing it.
            ByteBuffer outBB = outBBB.underlyingObject();
            // did it write something?
            int wpBBB = Maths.toUInt31(outBBB.writePosition());
            int length = wpBBB - outBB.limit();
            if (length >= TARGET_WRITE_SIZE) {
                outBB.limit(wpBBB);
                boolean busy2 = tryWrite(outBB);
                if (busy2)
                    busy = busy2;
                else
                    break;
            }
        } while (lastInBBBReadPosition != inBBB.readPosition());

        final ByteBuffer outBB = outBBB.underlyingObject();
        if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
            outBB.limit(Maths.toInt32(outBBB.writePosition()));
            busy |= tryWrite(outBB);
        }

        Jvm.safepoint();

        if (inBBB.readRemaining() == 0) {
            clearBuffer();

        } else if (inBBB.readPosition() > TCP_BUFFER / 4) {
            compactBuffer();
            busy = true;
        }

        return busy;
    }

    private void clearBuffer() {
        inBBB.clear();
        @Nullable final ByteBuffer inBB = inBBB.underlyingObject();
        inBB.clear();
    }

    private void compactBuffer() {
        // if it read some data compact();
        @Nullable final ByteBuffer inBB = inBBB.underlyingObject();
        inBB.position((int) inBBB.readPosition());
        inBB.limit((int) inBBB.readLimit());
        Jvm.safepoint();

        inBB.compact();
        Jvm.safepoint();
        inBBB.readPosition(0);
        inBBB.readLimit(inBB.remaining());
    }

    private void handleIOE(@NotNull final IOException e) {
        if (isClosed() || (tcpHandler != null && tcpHandler.hasClientClosed()))
            return;

        try {
            String message = e.getMessage();
            if (isAConnectionResetException(e)) {
                LOG.trace(message, e);
            } else if (message != null && message.startsWith("An existing connection was forcibly closed")) {
                Jvm.debug().on(getClass(), message);
            } else if (!(e instanceof ClosedByInterruptException)) {
                Jvm.warn().on(getClass(), "", e);
            }

            // The remote server has sent you a RST packet, which indicates an immediate dropping of the connection,
            // rather than the usual handshake. This bypasses the normal half-closed state transition.
            // I like this description: "Connection reset by peer" is the TCP/IP equivalent
            // of slamming the phone back on the hook.
            if (CALL_MISSED_HEARTBEAT_ON_DISCONNECT) {
                HeartbeatListener heartbeatListener = nc.heartbeatListener();
                if (heartbeatListener != null)
                    heartbeatListener.onMissedHeartbeat();
            }

        } finally {
            closeAndStartReconnector();
        }
    }

    @Override
    protected void performClose() {
        final T nc = this.nc;
        if (nc != null)
            closeQuietly(nc, nc.socketReconnector(), nc.networkStatsListener());
        closeQuietly(tcpHandler, sc);
    }

    boolean flushedOut(boolean flushed) {
        // callback to nc if moving to flushed (from not-flushed)
        if (flushed && !this.flushedOut)
            nc.onFlushed();

        this.flushedOut = flushed;
        return flushed;
    }

    @PackageLocal
    boolean tryWrite(final ByteBuffer outBB) throws IOException {
        if (flushedOut(outBB.remaining() <= 0))
            return false;

        final int start = outBB.position();
        final long beginNs = System.nanoTime();
        assert !sc.isBlocking();
        int wrote = sc.write(outBB);
        long elapsedNs = System.nanoTime() - beginNs;
        if (nbWarningEnabled && elapsedNs > NBW_WARNING_NANOS)
            statusMonitorEventHandler.add(
                    new ThreadLogTypeElapsedRecord(LogType.WRITE, elapsedNs, Affinity.getCpu(), Affinity.getAffinity()));

        tcpHandler.onWriteTime(beginNs, outBB, start, outBB.position());

        statusMonitorEventHandler.addBytesWritten((long) outBB.position() - start);
        writeLog.log(outBB, start, outBB.position());

        if (wrote < 0) {
            closeAndStartReconnector();
        } else if (wrote > 0) {
            outBB.compact().flip();
            outBBB.writePosition(outBB.limit());
            return true;
        }
        return false;
    }

    public boolean writeAction() {

        boolean busy = false;
        try {
            // get more data to write if the buffer was empty
            // or we can write some of what is there
            final ByteBuffer outBB = outBBB.underlyingObject();
            final int remaining = outBB.remaining();
            busy = remaining > 0;
            if (busy)
                tryWrite(outBB);

            // has the remaining changed, i.e. did it write anything?
            if (outBB.remaining() == remaining) {
                busy |= invokeHandler();
                if (!busy)
                    busy = tryWrite(outBB);
            }
        } catch (ClosedChannelException cce) {
            closeAndStartReconnector();
        } catch (IOException e) {
            handleIOE(e);
        }
        return busy;
    }

    private enum LogType {
        READ("read"), WRITE("write");

        private final String label;

        LogType(@NotNull final String label) {
            this.label = label;
        }

        private String label() {
            return label;
        }

    }

    @FunctionalInterface
    public interface SocketReader {

        /**
         * Reads content from the provided {@code socketChannel} into the provided {@code bytes}.
         *
         * @param socketChannel to read from
         * @param bytes         to which content from the {@code socketChannel} is put
         * @return the number of bytes read from the provided {@code socketChannel}.
         * @throws IOException if there is a problem reading form the provided {@code socketChannel}.
         */
        int read(@NotNull ChronicleSocketChannel socketChannel, @NotNull Bytes<ByteBuffer> bytes) throws IOException;
    }

    public static final class DefaultSocketReader implements SocketReader {

        @Override
        public int read(@NotNull final ChronicleSocketChannel socketChannel, @NotNull final Bytes<ByteBuffer> bytes) throws IOException {
            return socketChannel.read(bytes.underlyingObject());
        }
    }

    public static class Factory<T extends NetworkContext<T>> implements MarshallableFunction<T, TcpEventHandler<T>> {

        @NotNull
        @Override
        public TcpEventHandler<T> apply(@NotNull final T nc) {
            return new TcpEventHandler<>(nc);
        }
    }

    private static final class ThreadLogTypeElapsedRecord {

        private final LogType logType;
        private final long elapsedNs;
        private final int cpuId;
        private final BitSet affinity;

        /**
         * Does nothing, just called to load the class eagerly
         * <p>
         * We have observed lengthy class loading delays due to this class not being loaded
         */
        private static void loadClass() {
        }

        public ThreadLogTypeElapsedRecord(@NotNull final LogType logType,
                                          final long elapsedNs,
                                          final int cpuId,
                                          @Nullable final BitSet affinity) {
            this.logType = logType;
            this.elapsedNs = elapsedNs;
            this.cpuId = cpuId;
            this.affinity = affinity;
        }
    }

    /**
     * EventHandler that handles both network stats listening and printing of messages that otherwise might impact performance.
     */
    private final class StatusMonitorEventHandler implements EventHandler {

        private final String className;
        private final StringBuilder messageBuilder = new StringBuilder();
        private final AtomicInteger socketPollCount = new AtomicInteger();
        private final AtomicLong bytesReadCount = new AtomicLong();
        private final AtomicLong bytesWriteCount = new AtomicLong();
        private final Queue<ThreadLogTypeElapsedRecord> logs = new ConcurrentLinkedQueue<>();

        private long lastMonitor;

        public StatusMonitorEventHandler(@NotNull final Class<?> clazz) {
            this.className = clazz.getSimpleName();
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (TcpEventHandler.this.isClosed())
                throw InvalidEventHandlerException.reusable();

            if (!logs.isEmpty()) {
                ThreadLogTypeElapsedRecord msg;
                while ((msg = logs.poll()) != null) {
                    messageBuilder.setLength(0);
                    messageBuilder
                            .append("Non blocking ")
                            .append(className)
                            .append(" ")
                            .append(msg.logType.label())
                            .append(" took ")
                            .append(msg.elapsedNs / 1000)
                            .append(" us, CPU: ")
                            .append(msg.cpuId)
                            .append(", affinity ")
                            .append(msg.affinity);

                    // no point grabbing stack trace as thread has moved on

                    Jvm.perf().on(getClass(), messageBuilder.toString());
                }
            }

            final long now = nc.timeProvider().currentTimeMillis();
            if (now > lastMonitor + (MONITOR_POLL_EVERY_SEC * 1000L)) {
                final NetworkStatsListener<T> networkStatsListener = nc.networkStatsListener();
                if (networkStatsListener != null && !networkStatsListener.isClosed()) {
                    if (lastMonitor == 0) {
                        networkStatsListener.onNetworkStats(0, 0, 0);
                    } else {
                        networkStatsListener.onNetworkStats(
                                bytesWriteCount.get() / MONITOR_POLL_EVERY_SEC,
                                bytesReadCount.get() / MONITOR_POLL_EVERY_SEC,
                                socketPollCount.get() / MONITOR_POLL_EVERY_SEC);
                        bytesWriteCount.set(0);
                        bytesReadCount.set(0);
                        socketPollCount.set(0);
                    }
                }
                lastMonitor = now;
            }
            return false;
        }

        private void incrementSocketPollCount() {
            socketPollCount.incrementAndGet();
        }

        private void addBytesRead(long delta) {
            bytesReadCount.addAndGet(delta);
        }

        private void addBytesWritten(long delta) {
            bytesWriteCount.addAndGet(delta);
        }

        private void add(@NotNull TcpEventHandler.ThreadLogTypeElapsedRecord logTypeTimeRecord) {
            logs.add(logTypeTimeRecord);
        }

        @Override
        public @NotNull HandlerPriority priority() {
            return HandlerPriority.MONITOR;
        }
    }
}
