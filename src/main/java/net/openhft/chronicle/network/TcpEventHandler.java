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
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class TcpEventHandler implements EventHandler {
    public static final int CAPACITY = 1 << 23;
    @NotNull
    private final SocketChannel sc;
    private final TcpHandler handler;
    private final ByteBuffer inBB = ByteBuffer.allocateDirect(CAPACITY);
    private final Bytes inBBB = Bytes.wrapForRead(inBB.slice());
    private final ByteBuffer outBB = ByteBuffer.allocateDirect(CAPACITY);
    private final Bytes outBBB = Bytes.wrapForWrite(outBB.slice());
    private final SessionDetailsProvider sessionDetails;

    public TcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, final SessionDetailsProvider sessionDetails) throws IOException {
        this.sc = sc;
        sc.configureBlocking(false);
        sc.socket().setTcpNoDelay(true);
        sc.socket().setReceiveBufferSize(CAPACITY);
        sc.socket().setSendBufferSize(CAPACITY);

        this.handler = handler;
        // there is nothing which needs to be written by default.
        outBB.limit(0);
        this.sessionDetails = sessionDetails;
        // allow these to be used by another thread.
        // todo check that this can be commented out
        // inBBB.clearThreadAssociation();
        //  outBBB.clearThreadAssociation();

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
    public boolean runOnce() {
        try {
            int read = inBB.remaining() > 0 ? sc.read(inBB) : 1;
            if (read < 0) {
                closeSC();

            } else if (read > 0) {
                // inBB.position() where the data has been read() up to.
                invokeHandler();
            }
        } catch (IOException e) {
            handleIOE(e);
        }

        return false;
    }

    void invokeHandler() throws IOException {
        inBBB.readLimit(inBB.position());
        outBBB.writePosition(outBB.limit());
        handler.process(inBBB, outBBB, sessionDetails);

        // did it write something?
        if (outBBB.writePosition() > outBB.limit()) {
            outBB.limit(Maths.toInt32(outBBB.writePosition()));
            tryWrite();
        }
        // TODO Optimise.
        // if it read some data compact();
        if (inBBB.readPosition() > 0) {
            inBB.position((int) inBBB.readPosition());
            inBB.limit((int) inBBB.readLimit());
            inBB.compact();
            inBBB.readPosition(0);
            inBBB.readLimit(inBB.position());
        }
    }

    @Override
    public boolean isDead() {
        return !sc.isOpen();
    }

    void handleIOE(@NotNull IOException e) {
        e.printStackTrace();
        closeSC();
    }

    private void closeSC() {
        try {
            sc.close();
        } catch (IOException ignored) {
        }
    }

    boolean tryWrite() throws IOException {
        int wrote = sc.write(outBB);
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
        public boolean runOnce() {
            try {
                // get more data to write if the buffer was empty
                // or we can write some of what is there
                if (outBB.remaining() == 0 || tryWrite()) {
                    invokeHandler();
                    tryWrite();
                }
            } catch (ClosedChannelException cce) {
                closeSC();

            } catch (IOException e) {
                handleIOE(e);
            }
            return false;
        }

        @Override
        public boolean isDead() {
            return !sc.isOpen();
        }
    }
}
