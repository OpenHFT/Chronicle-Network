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
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class TcpEventHandler implements EventHandler, Closeable {
    public static final int TCP_BUFFER = 64 << 10;
    public static final int CAPACITY = 8 << 20;

    @NotNull
    private final SocketChannel sc;
    private final TcpHandler handler;
    private ByteBuffer inBB = ByteBuffer.allocateDirect(CAPACITY);
    private Bytes inBBB;
    private ByteBuffer outBB = ByteBuffer.allocateDirect(CAPACITY);
    private Bytes outBBB;
    private final SessionDetailsProvider sessionDetails;
    private final long heartBeatIntervalTicks;
    private final long heartBeatTimeoutTicks;
    private long lastTickReadTime = Time.tickTime(), lastHeartBeatTick = lastTickReadTime + 1000;
    private final NetworkLog readLog, writeLog;

    public TcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, final SessionDetailsProvider sessionDetails,
                           boolean unchecked, long heartBeatIntervalTicks, long heartBeatTimeoutTicks) throws IOException {
        this.heartBeatIntervalTicks = heartBeatIntervalTicks;
        this.heartBeatTimeoutTicks = heartBeatTimeoutTicks;
        assert heartBeatIntervalTicks <= heartBeatTimeoutTicks / 2;

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
        return HandlerPriority.HIGH;
    }

    @Override
    public void eventLoop(@NotNull EventLoop eventLoop) {
        // handle unsolicited or unfulfilled writes at a lower priority
        eventLoop.addHandler(new WriteEventHandler());
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

        try {
            int start = inBB.position();
            int read = inBB.remaining() > 0 ? sc.read(inBB) : 1;
            if (read < 0) {
                closeSC();

            } else if (read > 0) {
                WanSimulator.dataRead(read);
                readLog.log(inBB, start, inBB.position());
                lastTickReadTime = Time.tickTime();
                // inBB.position() where the data has been read() up to.
                return invokeHandler();
            } else {
                readLog.idle();
                long tickTime = Time.tickTime();
                if (tickTime > lastTickReadTime + heartBeatTimeoutTicks) {
                    handler.onEndOfConnection(true);
                    closeSC();
                    throw new InvalidEventHandlerException();
                }
                if (tickTime > lastHeartBeatTick + heartBeatIntervalTicks) {
                    lastHeartBeatTick = tickTime;
                    sendHeartBeat();
                }
            }
        } catch (IOException e) {
            handleIOE(e);
        }

        return false;
    }

    protected void sendHeartBeat() throws IOException {
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

    boolean invokeHandler() throws IOException {
        boolean busy = false;
        inBBB.readLimit(inBB.position());
        outBBB.writePosition(outBB.limit());
        handler.process(inBBB, outBBB, sessionDetails);

        // did it write something?
        if (outBBB.writePosition() > outBB.limit() || outBBB.writePosition() >= 4) {
            outBB.limit(Maths.toInt32(outBBB.writePosition()));
            busy |= tryWrite();
        }
        // TODO Optimise.
        // if it read some data compact();
        if (inBBB.readPosition() > 0) {
            inBB.position((int) inBBB.readPosition());
            inBB.limit((int) inBBB.readLimit());
            inBB.compact();
            inBBB.readPosition(0);
            inBBB.readLimit(inBB.position());
            busy = true;
        }
        return busy;
    }


    void handleIOE(@NotNull IOException e) {
        if (!(e instanceof ClosedByInterruptException))
            e.printStackTrace();
        closeSC();
    }

    @Override
    public void close() {
        closeSC();
    }

    private void closeSC() {
        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }

    boolean tryWrite() throws IOException {
        if (outBB.remaining() <= 0)
            return false;
        int start = outBB.position();
        int wrote = sc.write(outBB);
        writeLog.log(outBB, start, outBB.position());

        if (wrote < 0) {
            closeSC();

        } else if (wrote > 0) {
            outBB.compact().flip();
            outBBB.writePosition(outBB.limit());
            outBBB.writeLimit(outBB.capacity());
            return true;
        }
        return false;
    }

    class WriteEventHandler implements EventHandler {
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
                    invokeHandler();
                    if (!busy)
                        busy |= tryWrite();
                }
            } catch (ClosedChannelException cce) {
                closeSC();

            } catch (IOException e) {
                handleIOE(e);
            }
            return busy;
        }
    }
}
