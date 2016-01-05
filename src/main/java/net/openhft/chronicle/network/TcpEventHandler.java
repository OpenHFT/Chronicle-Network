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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import static net.openhft.chronicle.network.ServerThreadingStrategy.serverThreadingStrategy;

/**
 * Created by peter.lawrey on 22/01/15.
 */
class TcpEventHandler implements EventHandler, Closeable {
    public static final int TCP_BUFFER = Integer.getInteger("TcpEventHandler.tcpBufferSize", TcpChannelHub.BUFFER_SIZE);
    private static final Logger LOG = LoggerFactory.getLogger(TcpEventHandler.class);
    private static final int CAPACITY = Integer.getInteger("TcpEventHandler.capacity", TCP_BUFFER);

    @NotNull
    private final SocketChannel sc;
    private final TcpHandler handler;
    private final SessionDetailsProvider sessionDetails;
    private final long heartBeatIntervalTicks;
    private final long heartBeatTimeoutTicks;
    @NotNull
    private final WriteEventHandler writeEventHandler;
    @NotNull
    private final NetworkLog readLog, writeLog;
    int oneInTen;
    @Nullable
    private ByteBuffer inBB = ByteBuffer.allocateDirect(CAPACITY);
    @Nullable
    private Bytes inBBB;
    @Nullable
    private ByteBuffer outBB = ByteBuffer.allocateDirect(CAPACITY);
    @Nullable
    private Bytes outBBB;
    private long lastTickReadTime = Time.tickTime(), lastHeartBeatTick = lastTickReadTime + 1000;

    public TcpEventHandler(@NotNull SocketChannel sc, @NotNull TcpHandler handler, @NotNull final SessionDetailsProvider sessionDetails,
                           boolean unchecked, long heartBeatIntervalTicks, long heartBeatTimeoutTicks) throws IOException {
        this.heartBeatIntervalTicks = heartBeatIntervalTicks;
        this.heartBeatTimeoutTicks = heartBeatTimeoutTicks;
        assert heartBeatIntervalTicks <= heartBeatTimeoutTicks / 2;
        this.writeEventHandler = new WriteEventHandler();
        this.sc = sc;
        sc.configureBlocking(false);
        sc.socket().setTcpNoDelay(true);
        sc.socket().setReceiveBufferSize(TCP_BUFFER);
        sc.socket().setSendBufferSize(TCP_BUFFER);

        this.handler = handler;
        // there is nothing which needs to be written by default.
        this.sessionDetails = sessionDetails;
        // allow these to be used by another thread.
        // todo check that this can be commented out
        // inBBB.clearThreadAssociation();
        //  outBBB.clearThreadAssociation();

        assert inBB != null;
        inBBB = Bytes.wrapForRead(inBB.slice()).unchecked(unchecked);
        assert outBB != null;
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
    public boolean action() throws InvalidEventHandlerException {

        if (!sc.isOpen()) {
            handler.onEndOfConnection(false);

            // clear these to free up memory.
            inBB = outBB = null;
            inBBB = outBBB = null;
            throw new InvalidEventHandlerException();
        }

        if (oneInTen++ == 10) {
            oneInTen = 0;
            try {
                writeEventHandler.action();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }

        try {

            assert inBB != null;
            int start = inBB.position();
            int read = inBB.remaining() > 0 ? sc.read(inBB) : Integer.MAX_VALUE;

            if (read < 0) {
                closeSC();
                throw new InvalidEventHandlerException();
                //return false;
            }

            if (read > 0) {
                WanSimulator.dataRead(read);
                //    if (Jvm.isDebug())
                //        System.out.println("Read: " + read + " start: " + start + " pos: " + inBB
                //       .position());
                readLog.log(inBB, start, inBB.position());
                lastTickReadTime = Time.tickTime();
                // inBB.position() where the data has been read() up to.
                return invokeHandler();
            }

            readLog.idle();

            if (heartBeatIntervalTicks == 0)
                return false;

            long tickTime = Time.tickTime();
            if (tickTime > lastTickReadTime + heartBeatTimeoutTicks) {
                closeSC();
                throw new InvalidEventHandlerException();
                // return false;
            }

            if (tickTime > lastHeartBeatTick + heartBeatIntervalTicks) {
                lastHeartBeatTick = tickTime;
                sendHeartBeat();
            }
        } catch (ClosedChannelException e) {

            closeSC();
        } catch (IOException e) {
            handleIOE(e, handler.hasClientClosed());
        }

        return false;
    }

    private void sendHeartBeat() throws IOException {
        if (LOG.isDebugEnabled())
            LOG.debug("sendHeartbeat - " + sc.getRemoteAddress());
        assert outBB != null;
        assert outBBB != null;
        outBBB.writePosition(outBB.limit());
        handler.sendHeartBeat(outBBB, sessionDetails);

        // did it write something?
        if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
            outBB.limit(Maths.toInt32(outBBB.writePosition()));
            tryWrite();
        } else {
            writeLog.idle();
        }
    }

    private boolean invokeHandler() throws IOException {
        boolean busy = false;
        assert inBB != null;
        assert inBBB != null;
        inBBB.readLimit(inBB.position());
        assert outBB != null;
        assert outBBB != null;
        outBBB.writePosition(outBB.limit());
        long lastInBBBReadPosition;
        do {
            lastInBBBReadPosition = inBBB.readPosition();
            handler.process(inBBB, outBBB, sessionDetails);
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
//            if (inBBB.readPosition() *2 > inBBB.realCapacity() || inBBB.readRemaining() <= 128) {
            inBB.compact();
            inBBB.readPosition(0);
            inBBB.readLimit(inBB.position());
//            }

            busy = true;
        }
        return busy;
    }

    private void handleIOE(@NotNull IOException e, final boolean clientIntentionallyClosed) {
        try {

            if (clientIntentionallyClosed)
                return;

            if (e.getMessage() != null && e.getMessage().startsWith("An existing connection was " +
                    "forcibly closed"))
                LOG.warn(e.getMessage());
            else if (!(e instanceof ClosedByInterruptException))
                LOG.error("", e);

        } finally {
            closeSC();
        }
    }

    @Override
    public void close() {
        closeSC();
    }

    private void closeSC() {
        try {
            handler.close();
        } catch (Exception ignored) {
        }

        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }

    boolean tryWrite() throws IOException {
        assert outBB != null;
        if (outBB.remaining() <= 0)
            return false;
        int start = outBB.position();
        int wrote = sc.write(outBB);

        writeLog.log(outBB, start, outBB.position());

        if (wrote < 0) {
            closeSC();
        } else if (wrote > 0) {
            lastTickReadTime = Time.tickTime();
            outBB.compact().flip();
            assert outBBB != null;
            outBBB.writeLimit(outBB.capacity());
            outBBB.writePosition(outBB.limit());
            return true;
        }
        return false;
    }

    private class WriteEventHandler implements EventHandler {
        @Override
        public boolean action() throws InvalidEventHandlerException {
            if (!sc.isOpen()) throw new InvalidEventHandlerException();

            boolean busy = false;
            try {
                // get more data to write if the buffer was empty
                // or we can write some of what is there
                assert outBB != null;
                int remaining = outBB.remaining();
                busy = remaining > 0;
                if (busy)
                    tryWrite();
                if (outBB.remaining() == remaining) {
                    invokeHandler();
                    if (!busy)
                        busy |= tryWrite();
                }
            } catch (ClosedChannelException cce) {
                closeSC();

            } catch (IOException e) {
                handleIOE(e, handler.hasClientClosed());
            }
            return busy;
        }
    }
}
