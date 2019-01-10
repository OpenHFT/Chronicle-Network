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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.tcp.ISocketChannel;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;

public class TcpEventHandler implements EventHandler, Closeable, TcpEventHandlerManager {

    private static final int MONITOR_POLL_EVERY_SEC = Integer.getInteger("tcp.event.monitor.secs", 10);
    private static final Logger LOG = LoggerFactory.getLogger(TcpEventHandler.class);
    private static final AtomicBoolean FIRST_HANDLER = new AtomicBoolean();
    public static boolean DISABLE_TCP_NODELAY;

    static {
        DISABLE_TCP_NODELAY = Boolean.getBoolean("disable.tcp_nodelay");
        if (DISABLE_TCP_NODELAY) System.out.println("tcpNoDelay disabled");
    }

    @NotNull
    private final ISocketChannel sc;
    @NotNull
    private final NetworkContext nc;
    @NotNull
    private final WriteEventHandler writeEventHandler;
    @NotNull
    private final NetworkLog readLog, writeLog;
    @NotNull
    private final Bytes<ByteBuffer> inBBB;
    @NotNull
    private final Bytes<ByteBuffer> outBBB;
    private final boolean fair;
    private int oneInTen;
    private volatile boolean isCleaned;
    @Nullable
    private volatile TcpHandler tcpHandler;
    private long lastTickReadTime = System.currentTimeMillis();
    private volatile boolean closed;
    // monitoring
    private int socketPollCount;
    private long bytesReadCount;
    private long bytesWriteCount;
    private long lastMonitor;

    public TcpEventHandler(@NotNull NetworkContext nc) {
        this(nc, false);
    }

    public TcpEventHandler(@NotNull NetworkContext nc, boolean fair) {

        this.writeEventHandler = new WriteEventHandler();
        this.sc = ISocketChannel.wrapUnsafe(nc.socketChannel());
        this.nc = nc;
        this.fair = fair;

        try {
            sc.configureBlocking(false);
            Socket sock = sc.socket();
            // TODO: should have a strategy for this like ConnectionStrategy
            if (!DISABLE_TCP_NODELAY)
                sock.setTcpNoDelay(true);

            if (TCP_BUFFER >= 64 << 10) {
                sock.setReceiveBufferSize(TCP_BUFFER);
                sock.setSendBufferSize(TCP_BUFFER);

                checkBufSize(sock.getReceiveBufferSize(), "recv");
                checkBufSize(sock.getSendBufferSize(), "send");
            }
        } catch (IOException e) {
            Jvm.warn().on(getClass(), e);
        }
        // allow these to be used by another thread.
        // todo check that this can be commented out

        inBBB = Bytes.elasticByteBuffer(TCP_BUFFER + OS.pageSize());
        outBBB = Bytes.elasticByteBuffer(TCP_BUFFER);

        // TODO FIX, these are not being released on close.
        assert BytesUtil.unregister(inBBB);
        assert BytesUtil.unregister(outBBB);

        // must be set after we take a slice();
        outBBB.underlyingObject().limit(0);
        readLog = new NetworkLog(this.sc, "read");
        writeLog = new NetworkLog(this.sc, "write");
        if (FIRST_HANDLER.compareAndSet(false, true))
            warmUp();
    }

    public void warmUp() {
        System.out.println(TcpEventHandler.class.getSimpleName() + " - Warming up...");
        int runs = 12000;
        long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            inBBB.readPositionRemaining(8, 1024);
            compactBuffer();
            clearBuffer();
        }
        long time = System.nanoTime() - start;
        System.out.println(TcpEventHandler.class.getSimpleName() + " - ... warmed up - took " + (time / runs / 1e3) + " us avg");
    }

    private void checkBufSize(int bufSize, String name) {
        if (bufSize < TCP_BUFFER) {
            LOG.warn("Attempted to set " + name + " tcp buffer to " + TCP_BUFFER + " but kernel only allowed " + bufSize);
        }
    }

    public ISocketChannel socketChannel() {
        return sc;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        ServerThreadingStrategy sts = nc.serverThreadingStrategy();
        switch (sts) {

            case SINGLE_THREADED:
                return HandlerPriority.MEDIUM;
            case CONCURRENT:
                return HandlerPriority.CONCURRENT;
            default:
                throw new UnsupportedOperationException("todo");
        }
    }

    @Nullable
    public TcpHandler tcpHandler() {
        return tcpHandler;
    }

    @Override
    public void tcpHandler(TcpHandler tcpHandler) {
        nc.onHandlerChanged(tcpHandler);
        this.tcpHandler = tcpHandler;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        Jvm.optionalSafepoint();
        return action0();
    }

    public synchronized boolean action0() throws InvalidEventHandlerException {
        Jvm.optionalSafepoint();
        if (tcpHandler == null)
            return false;

        if (closed) {
            // close();
            throw new InvalidEventHandlerException();
        }

        if (!sc.isOpen()) {
            tcpHandler.onEndOfConnection(false);
            Closeable.closeQuietly(nc);
            // clear these to free up memory.
            throw new InvalidEventHandlerException("socket is closed");
        }

        socketPollCount++;

        boolean busy = false;
        if (fair || oneInTen++ >= 8) {
            oneInTen = 0;
            try {
                busy = writeEventHandler.action();
            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        }
        try {
            ByteBuffer inBB = inBBB.underlyingObject();
            int start = inBB.position();

            int read = inBB.remaining() > 0 ? sc.read(inBB) : Integer.MAX_VALUE;

            if (read == Integer.MAX_VALUE)
                LOG.trace("inBB is full, can't read from socketChannel");
            if (read > 0) {
                WanSimulator.dataRead(read);
                tcpHandler.onReadTime(System.nanoTime(), inBB, start, inBB.position());
                lastTickReadTime = System.currentTimeMillis();
                readLog.log(inBB, start, inBB.position());
                if (invokeHandler())
                    oneInTen++;
                busy = true;
            } else if (read == 0) {
                if (outBBB.readRemaining() > 0) {
                    busy |= invokeHandler();
                }

                // check for timeout only here - in other branches we either just read something or are about to close socket anyway
                if (nc.heartbeatTimeoutMs() > 0) {
                    long tickTime = System.currentTimeMillis();
                    if (tickTime > lastTickReadTime + nc.heartbeatTimeoutMs()) {
                        final HeartbeatListener heartbeatListener = nc.heartbeatListener();
                        if (heartbeatListener != null && heartbeatListener.onMissedHeartbeat()) {
                            // implementater tries to recover - do not disconnect for some time
                            lastTickReadTime += heartbeatListener.lingerTimeBeforeDisconnect();
                        } else {
                            tcpHandler.onEndOfConnection(true);
                            closeSC();
                            throw new InvalidEventHandlerException("heartbeat timeout");
                        }
                    }
                }

                if (!busy)
                    monitorStats();
            } else {
                close();
                throw new InvalidEventHandlerException("socket closed " + sc);
            }

            return busy;

        } catch (ClosedChannelException e) {
            close();
            throw new InvalidEventHandlerException(e);
        } catch (IOException e) {
            close();
            handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            throw new InvalidEventHandlerException();
        } catch (InvalidEventHandlerException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            Jvm.warn().on(getClass(), "", e);
            throw new InvalidEventHandlerException(e);
        }
    }

    private void monitorStats() {
        // TODO: consider installing this on EventLoop using Timer
        long now = System.currentTimeMillis();
        if (now > lastMonitor + (MONITOR_POLL_EVERY_SEC * 1000)) {
            final NetworkStatsListener networkStatsListener = nc.networkStatsListener();
            if (networkStatsListener != null) {
                if (lastMonitor == 0) {
                    networkStatsListener.onNetworkStats(0, 0, 0);
                } else {
                    networkStatsListener.onNetworkStats(
                            bytesWriteCount / MONITOR_POLL_EVERY_SEC,
                            bytesReadCount / MONITOR_POLL_EVERY_SEC,
                            socketPollCount / MONITOR_POLL_EVERY_SEC);
                    bytesWriteCount = bytesReadCount = socketPollCount = 0;
                }
            }
            lastMonitor = now;
        }
    }

    private synchronized void clean() {

        if (isCleaned)
            return;
        isCleaned = true;
        final long usedDirectMemory = Jvm.usedDirectMemory();
        IOTools.clean(inBBB.underlyingObject());
        IOTools.clean(outBBB.underlyingObject());

        if (usedDirectMemory == Jvm.usedDirectMemory())
            Jvm.warn().on(getClass(), "nothing cleaned");

    }

    @PackageLocal
    boolean invokeHandler() throws IOException {
        Jvm.optionalSafepoint();
        boolean busy = false;
        final int position = inBBB.underlyingObject().position();
        inBBB.readLimit(position);
        outBBB.writePosition(outBBB.underlyingObject().limit());

        long lastInBBBReadPosition;
        do {
            lastInBBBReadPosition = inBBB.readPosition();
            tcpHandler.process(inBBB, outBBB, nc);
            bytesReadCount += (inBBB.readPosition() - lastInBBBReadPosition);

            // process method might change the underlying ByteBuffer by resizing it.
            ByteBuffer outBB = outBBB.underlyingObject();
            // did it write something?
            if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
                outBB.limit(Maths.toInt32(outBBB.writePosition()));
                busy |= tryWrite(outBB);
                break;
            }
        } while (lastInBBBReadPosition != inBBB.readPosition());

        Jvm.optionalSafepoint();

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
        @Nullable ByteBuffer inBB = inBBB.underlyingObject();
        inBB.clear();
    }

    private void compactBuffer() {
        // if it read some data compact();
        @Nullable ByteBuffer inBB = inBBB.underlyingObject();
        inBB.position((int) inBBB.readPosition());
        inBB.limit((int) inBBB.readLimit());
        Jvm.optionalSafepoint();

        inBB.compact();
        Jvm.optionalSafepoint();
        inBBB.readPosition(0);
        inBBB.readLimit(inBB.remaining());
    }

    private void handleIOE(@NotNull IOException e, final boolean clientIntentionallyClosed,
                           @Nullable HeartbeatListener heartbeatListener) {
        try {

            if (clientIntentionallyClosed)
                return;
            if (e.getMessage() != null && e.getMessage().startsWith("Connection reset by peer"))
                LOG.trace("", e.getMessage());
            else if (e.getMessage() != null && e.getMessage().startsWith("An existing connection " +
                    "was forcibly closed"))
                Jvm.debug().on(getClass(), e.getMessage());

            else if (!(e instanceof ClosedByInterruptException))
                Jvm.warn().on(getClass(), "", e);

            // The remote server has sent you a RST packet, which indicates an immediate dropping of the connection,
            // rather than the usual handshake. This bypasses the normal half-closed state transition.
            // I like this description: "Connection reset by peer" is the TCP/IP equivalent
            // of slamming the phone back on the hook.

            if (heartbeatListener != null)
                heartbeatListener.onMissedHeartbeat();

        } finally {
            closeSC();
        }
    }

    @Override
    public void close() {
        closed = true;
        closeSC();
        clean();
    }

    @PackageLocal
    void closeSC() {
        Closeable.closeQuietly(tcpHandler);
        Closeable.closeQuietly(this.nc.networkStatsListener());
        Closeable.closeQuietly(sc);
        Closeable.closeQuietly(nc);
    }

    @PackageLocal
    boolean tryWrite(ByteBuffer outBB) throws IOException {
        if (outBB.remaining() <= 0)
            return false;
        int start = outBB.position();
        long writeTime = System.nanoTime();
        assert !sc.isBlocking();
        int wrote = sc.write(outBB);
        tcpHandler.onWriteTime(writeTime, outBB, start, outBB.position());

        bytesWriteCount += (outBB.position() - start);
        writeLog.log(outBB, start, outBB.position());

        if (wrote < 0) {
            closeSC();
        } else if (wrote > 0) {
            outBB.compact().flip();
            outBBB.writePosition(outBB.limit());
            return true;
        }
        return false;
    }

    public static class Factory implements MarshallableFunction<NetworkContext, TcpEventHandler> {
        public Factory() {
        }

        @NotNull
        @Override
        public TcpEventHandler apply(@NotNull NetworkContext nc) {
            return new TcpEventHandler(nc);
        }
    }

    private class WriteEventHandler implements EventHandler {

        @Override
        public boolean action() throws InvalidEventHandlerException {
            Jvm.optionalSafepoint();
            if (!sc.isOpen()) throw new InvalidEventHandlerException("socket is closed");

            boolean busy = false;
            try {
                // get more data to write if the buffer was empty
                // or we can write some of what is there
                ByteBuffer outBB = outBBB.underlyingObject();
                int remaining = outBB.remaining();
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
                closeSC();

            } catch (IOException e) {
                if (!closed)
                    handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            }
            return busy;
        }

    }
}
