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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class TcpEventHandler implements EventHandler, Closeable, TcpEventHandlerManager {

    public static final int TCP_BUFFER = TcpChannelHub.TCP_BUFFER;
    private static final Logger LOG = LoggerFactory.getLogger(TcpEventHandler.class);
    @NotNull
    private final SocketChannel sc;
    @NotNull
    private final NetworkContext nc;
    @NotNull
    private final SessionDetailsProvider sessionDetails;
    @NotNull
    private final WriteEventHandler writeEventHandler;
    @NotNull
    private final NetworkLog readLog, writeLog;

    @NotNull
    private final Bytes<ByteBuffer> inBBB;

    @NotNull
    private final Bytes<ByteBuffer> outBBB;
    private int oneInTen;
    private volatile boolean isCleaned;
    @Nullable
    private volatile TcpHandler tcpHandler;
    private long lastTickReadTime = Time.tickTime();

    private volatile boolean closed;

    public TcpEventHandler(@NotNull NetworkContext nc) {

        this.writeEventHandler = new WriteEventHandler();
        this.sc = nc.socketChannel();
        this.nc = nc;

        try {
            sc.configureBlocking(false);
            sc.socket().setTcpNoDelay(true);
            if (TCP_BUFFER >= 64 << 10) {
                sc.socket().setReceiveBufferSize(TCP_BUFFER);
                sc.socket().setSendBufferSize(TCP_BUFFER);
            }
        } catch (IOException e) {
            Jvm.warn().on(getClass(), e);
        }
        // there is nothing which needs to be written by default.
        this.sessionDetails = new VanillaSessionDetails();
        try {
            sessionDetails.clientAddress((InetSocketAddress) sc.getRemoteAddress());
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        // allow these to be used by another thread.
        // todo check that this can be commented out

        inBBB = Bytes.elasticByteBuffer(TCP_BUFFER + OS.pageSize());
        outBBB = Bytes.elasticByteBuffer(TCP_BUFFER);

        // must be set after we take a slice();
        outBBB.underlyingObject().limit(0);
        readLog = new NetworkLog(this.sc, "read");
        writeLog = new NetworkLog(this.sc, "write");
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

            case MULTI_THREADED_BUSY_WAITING:
                return HandlerPriority.BLOCKING;

            default:
                throw new UnsupportedOperationException("todo");
        }
    }

    @Override
    public void tcpHandler(TcpHandler tcpHandler) {
        nc.onHandlerChanged(tcpHandler);
        this.tcpHandler = tcpHandler;
    }

    @Override
    public synchronized boolean action() throws InvalidEventHandlerException {
        final HeartbeatListener heartbeatListener = nc.heartbeatListener();

        if (tcpHandler == null)
            return false;

        if (!sc.isOpen()) {
            tcpHandler.onEndOfConnection(false);
            Closeable.closeQuietly(nc);
            // clear these to free up memory.
            throw new InvalidEventHandlerException("socket is closed");
        } else if (closed) {
            Closeable.closeQuietly(nc);
            throw new InvalidEventHandlerException();
        }

        boolean busy = false;
        if (oneInTen++ >= 8) {
            oneInTen = 0;
            try {
                busy |= writeEventHandler.action();
            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        }
        try {
            @Nullable ByteBuffer inBB = inBBB.underlyingObject();
            int start = inBB.position();

            int read = inBB.remaining() > 0 ? sc.read(inBB) : Integer.MAX_VALUE;
//            if (read < Integer.MAX_VALUE && read != 0)
//                System.out.println("read " + read);

            if (read > 0) {
                WanSimulator.dataRead(read);
                tcpHandler.onReadTime(System.nanoTime());
                lastTickReadTime = Time.tickTime();
                //    if (Jvm.isDebug())
                //        System.out.println("Read: " + read + " start: " + start + " pos: " + inBB
                //       .position());
                readLog.log(inBB, start, inBB.position());
                // inBB.position() where the data has been read() up to.
                if (invokeHandler())
                    oneInTen++;
                return true;
            }
            if (read < 0) {
                close();
                throw new InvalidEventHandlerException("socket closed " + sc);
            }

            readLog.idle();

            if (nc.heartbeatTimeoutMs() == 0)
                return busy;

            long tickTime = Time.tickTime();
            if (tickTime > lastTickReadTime + nc.heartbeatTimeoutMs()) {

                if (heartbeatListener != null)
                    nc.heartbeatListener().onMissedHeartbeat();
                closeSC();
                throw new InvalidEventHandlerException("heatbeat timeout");
            }

        } catch (ClosedChannelException e) {
            closeSC();
            throw new InvalidEventHandlerException(e);
        } catch (IOException e) {
            closeSC();
            handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            throw new InvalidEventHandlerException();
        } catch (InvalidEventHandlerException e) {
            closeSC();
            throw e;
        } catch (Exception e) {
            closeSC();
            Jvm.warn().on(getClass(), "", e);
            throw new InvalidEventHandlerException(e);
        }

        return busy;
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

    boolean invokeHandler() throws IOException {
        boolean busy = false;
        final int position = inBBB.underlyingObject().position();
        inBBB.readLimit(position);
        outBBB.writePosition(outBBB.underlyingObject().limit());

        long lastInBBBReadPosition;
        do {
            lastInBBBReadPosition = inBBB.readPosition();
            tcpHandler.process(inBBB, outBBB);

            // did it write something?
            if (outBBB.writePosition() > outBBB.underlyingObject().limit() || outBBB.writePosition() >= 4) {
                outBBB.underlyingObject().limit(Maths.toInt32(outBBB.writePosition()));
                busy |= tryWrite();
                break;
            }
        } while (lastInBBBReadPosition != inBBB.readPosition());

        if (inBBB.readRemaining() == 0) {
            inBBB.clear();
            @Nullable ByteBuffer inBB = inBBB.underlyingObject();
            inBB.clear();

        } else if (inBBB.readPosition() > 0) {
            // if it read some data compact();
            @Nullable ByteBuffer inBB = inBBB.underlyingObject();
            inBB.position((int) inBBB.readPosition());
            inBB.limit((int) inBBB.readLimit());
            inBB.compact();
            inBBB.readPosition(0);
            inBBB.readLimit(inBB.remaining());

//            System.out.println("Compact " + inBBB.readPosition() + " rem " + inBBB.readRemaining() + " " + Wires.lengthOf(inBBB.readInt(inBBB.readPosition())));

            busy = true;

        } else if (inBBB.readPosition() > 0) {
            System.out.println("pos " + inBBB.readPosition());
        }

        return busy;
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

    private void closeSC() {
        Closeable.closeQuietly(this.nc.networkStatsListener());
        Closeable.closeQuietly(tcpHandler);
        Closeable.closeQuietly(sc);
        Closeable.closeQuietly(nc);
    }

    private boolean tryWrite() throws IOException {
        if (outBBB.underlyingObject().remaining() <= 0)
            return false;
        int start = outBBB.underlyingObject().position();
        long writeTickTime = Time.tickTime();
        long writeTime = System.nanoTime();
        assert !sc.isBlocking();
        int wrote = sc.write(outBBB.underlyingObject());
//        if (wrote > 200)
//        System.out.println("w "+wrote);
        tcpHandler.onWriteTime(writeTime);

        writeLog.log(outBBB.underlyingObject(), start, outBBB.underlyingObject().position());

        if (wrote < 0) {
            closeSC();
        } else if (wrote > 0) {
            lastTickReadTime = writeTickTime;
            outBBB.underlyingObject().compact().flip();
            outBBB.writePosition(outBBB.underlyingObject().limit());
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
            if (!sc.isOpen()) throw new InvalidEventHandlerException("socket is closed");

            boolean busy = false;
            try {
                // get more data to write if the buffer was empty
                // or we can write some of what is there
                int remaining = outBBB.underlyingObject().remaining();
                busy = remaining > 0;
                if (busy)
                    tryWrite();

                if (outBBB.underlyingObject().remaining() == remaining) {
                    busy |= invokeHandler();
                    if (!busy)
                        busy = tryWrite();
                }
            } catch (ClosedChannelException cce) {
                closeSC();

            } catch (IOException e) {
                if (!closed)
                    handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            }
            return busy;
        }

        // public HandlerPriority priority() {
        //   return HandlerPriority.CONCURRENT;
        //}
    }
}
