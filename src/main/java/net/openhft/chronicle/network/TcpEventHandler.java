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
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.network.ServerThreadingStrategy.serverThreadingStrategy;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class TcpEventHandler implements EventHandler, Closeable, TcpEventHandlerManager {

    static final int TCP_BUFFER = Integer.getInteger("TcpEventHandler.tcpBufferSize", TcpChannelHub.BUFFER_SIZE);
    private static final Logger LOG = LoggerFactory.getLogger(TcpEventHandler.class);
    private static final int CAPACITY = Integer.getInteger("TcpEventHandler.capacity", TCP_BUFFER);
    @NotNull
    private final SocketChannel sc;
    private final NetworkContext nc;
    private final SessionDetailsProvider sessionDetails;
    @NotNull
    private final WriteEventHandler writeEventHandler;
    @NotNull
    private final NetworkLog readLog, writeLog;
    @NotNull
    private final ByteBuffer inBB = allocateDirect(CAPACITY);
    @NotNull
    private final Bytes inBBB;
    @NotNull
    private final ByteBuffer outBB = allocateDirect(CAPACITY);
    @NotNull
    private final Bytes outBBB;
    private int oneInTen;
    private volatile boolean isCleaned;
    @Nullable
    private volatile TcpHandler tcpHandler;
    private long lastTickReadTime = Time.tickTime();
    private volatile boolean closed;
    public TcpEventHandler(@NotNull NetworkContext nc) throws IOException {
        final boolean unchecked = nc.isUnchecked();
        this.writeEventHandler = new WriteEventHandler();
        this.sc = nc.socketChannel();
        this.nc = nc;
        sc.configureBlocking(false);


        try {
            sc.socket().setTcpNoDelay(true);
            sc.socket().setReceiveBufferSize(TCP_BUFFER);
            sc.socket().setSendBufferSize(TCP_BUFFER);
        } catch (SocketException e) {
            LOG.info("", e);
        }
        // there is nothing which needs to be written by default.
        this.sessionDetails = new VanillaSessionDetails();
        sessionDetails.clientAddress((InetSocketAddress) sc.getRemoteAddress());
        // allow these to be used by another thread.
        // todo check that this can be commented out
        // inBBB.clearThreadAssociation();
        //  outBBB.clearThreadAssociation();

        inBBB = Bytes.wrapForRead(inBB.slice()).unchecked(unchecked);
        outBBB = Bytes.wrapForWrite(outBB.slice()).unchecked(unchecked);
        // must be set after we take a slice();
        outBB.limit(0);
        readLog = new NetworkLog(this.sc, "read");
        writeLog = new NetworkLog(this.sc, "write");
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        switch (serverThreadingStrategy()) {

            case SINGLE_THREADED:
                return HandlerPriority.HIGH;

            case MULTI_THREADED_BUSY_WAITING:
                return HandlerPriority.BLOCKING;

            default:
                throw new UnsupportedOperationException("todo");
        }
    }

    @Override
    public void tcpHandler(TcpHandler tcpHandler) {
        this.tcpHandler = tcpHandler;
    }

    @Override
    public synchronized boolean action() throws InvalidEventHandlerException {

        final HeartbeatListener heartbeatListener = nc.heartbeatListener();

        if (tcpHandler == null)
            return false;

        if (!sc.isOpen()) {
            tcpHandler.onEndOfConnection(false);

            // clear these to free up memory.
            throw new InvalidEventHandlerException();
        } else if (closed) {
            throw new InvalidEventHandlerException();
        }

        boolean busy = false;
        if (oneInTen++ >= 8) {
            oneInTen = 0;
            try {
                busy |= writeEventHandler.action();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }

        try {
            int start = inBB.position();
            int read = inBB.remaining() > 0 ? sc.read(inBB) : Integer.MAX_VALUE;

            if (read > 0) {
                WanSimulator.dataRead(read);
                tcpHandler.onReadTime(System.nanoTime());
                lastTickReadTime = Time.tickTime();
                //    if (Jvm.isDebug())
                //        System.out.println("Read: " + read + " start: " + start + " pos: " + inBB
                //       .position());
                readLog.log(inBB, start, inBB.position());
                // inBB.position() where the data has been read() up to.
                busy |= invokeHandler();
                return busy;
            }

            if (read < 0) {
                closeSC();
                throw new InvalidEventHandlerException();
                //return false;
            }

            readLog.idle();

            if (nc.heartbeatTimeoutMs() == 0)
                return busy;

            long tickTime = Time.tickTime();
            if (tickTime > lastTickReadTime + nc.heartbeatTimeoutMs()) {

                if (heartbeatListener != null)
                    nc.heartbeatListener().onMissedHeartbeat();
                closeSC();
                throw new InvalidEventHandlerException();
            }

        } catch (ClosedChannelException e) {
            closeSC();
            throw new InvalidEventHandlerException();
        } catch (IOException e) {
            handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            throw new InvalidEventHandlerException();
        } catch (InvalidEventHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("", e);
        }


        return busy;
    }

    private synchronized void clean() {

        if (isCleaned)
            return;
        isCleaned = true;
        final long usedDirectMemory = Jvm.usedDirectMemory();
        IOTools.clean(inBB);
        IOTools.clean(outBB);

        if (usedDirectMemory == Jvm.usedDirectMemory())
            LOG.error("nothing cleaned");

    }

    boolean invokeHandler() throws IOException {

        boolean busy = false;
//         long start = System.nanoTime();

        inBBB.readLimit(inBB.position());

        outBBB.writePosition(outBB.limit());
        long lastInBBBReadPosition;
        do {
            lastInBBBReadPosition = inBBB.readPosition();
            tcpHandler.process(inBBB, outBBB);
            // did it write something?
            if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
                outBB.limit(Maths.toInt32(outBBB.writePosition()));
                busy |= tryWrite();
                break;
            }
        } while (lastInBBBReadPosition != inBBB.readPosition());

        // TODO Optimise.
        // if it read some data compact();
        if (inBBB.readPosition() > 0) {
            inBB.position((int) inBBB.readPosition());
            inBB.limit((int) inBBB.readLimit());
            inBB.compact();
            inBBB.readPosition(0);

            busy = true;
        }
//         long time = System.nanoTime() - start;
//         if (Thread.currentThread().getName().startsWith("tree-1"))
//         System.out.println(LocalTime.now()+" "+busy+" "+time/1000/1e3);
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
                LOG.warn(e.getMessage());
            else if (!(e instanceof ClosedByInterruptException))
                LOG.error("", e);

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

        try {
            tcpHandler.close();
        } catch (Exception ignored) {
        }

        try {
            sc.close();
        } catch (IOException ignored) {
        }

    }

    private boolean tryWrite() throws IOException {

        if (outBB.remaining() <= 0)
            return false;
        int start = outBB.position();
        long writeTickTime = Time.tickTime();
        long writeTime = System.nanoTime();
        assert !sc.isBlocking();
        int wrote = sc.write(outBB);
        tcpHandler.onWriteTime(writeTime);

        writeLog.log(outBB, start, outBB.position());

        if (wrote < 0) {
            closeSC();
        } else if (wrote > 0) {
            lastTickReadTime = writeTickTime;
            outBB.compact().flip();
            outBBB.writeLimit(outBB.capacity());
            outBBB.writePosition(outBB.limit());
            return true;
        }
        return false;
    }

    public static class Factory implements MarshallableFunction<NetworkContext, TcpEventHandler> {
        private Factory(WireIn wireIn) {
            System.out.println(wireIn);
        }

        public Factory() {
        }

        @Override
        public TcpEventHandler apply(NetworkContext nc) {
            try {
                return new TcpEventHandler(nc);
            } catch (IOException e) {
                throw Jvm.rethrow(e);

            }
        }
    }

    private class WriteEventHandler implements EventHandler {
        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (!sc.isOpen()) throw new InvalidEventHandlerException();

            boolean busy = false;
            try {
                // get more data to write if the buffer was empty
                // or we can write some of what is there
                int remaining = outBB.remaining();
                busy = remaining > 0;
                if (busy)
                    tryWrite();
                if (outBB.remaining() == remaining) {
                    busy |= invokeHandler();
                    if (!busy)
                        busy = tryWrite();
                }
            } catch (ClosedChannelException cce) {
                closeSC();

            } catch (IOException e) {
                handleIOE(e, tcpHandler.hasClientClosed(), nc.heartbeatListener());
            }
            return busy;
        }
    }
}
