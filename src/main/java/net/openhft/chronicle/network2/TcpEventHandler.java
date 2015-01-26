package net.openhft.chronicle.network2;

import net.openhft.chronicle.network2.event.EventHandler;
import net.openhft.chronicle.network2.event.EventLoop;
import net.openhft.chronicle.network2.event.HandlerPriority;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * Created by peter on 22/01/15.
 */
public class TcpEventHandler implements EventHandler {
    public static final int CAPACITY = 128 * 1024;
    private final SocketChannel sc;
    private final TcpHandler handler;
    private final ByteBuffer inBB = ByteBuffer.allocateDirect(CAPACITY);
    private final Bytes inBBB = ByteBufferBytes.wrap(inBB.slice());
    private final ByteBuffer outBB = ByteBuffer.allocateDirect(CAPACITY);
    private final Bytes outBBB = ByteBufferBytes.wrap(outBB.slice());

    public TcpEventHandler(SocketChannel sc, TcpHandler handler) throws IOException {
        this.sc = sc;
        sc.configureBlocking(false);
        sc.socket().setTcpNoDelay(true);
        this.handler = handler;
        // there is nothing which needs to be written by default.
        outBB.limit(0);
    }

    @Override
    public HandlerPriority priority() {
        return HandlerPriority.HIGH;
    }

    @Override
    public void eventLoop(EventLoop eventLoop) {
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
        inBBB.limit(inBB.position());
        outBBB.position(outBB.limit());
        handler.process(inBBB, outBBB);

        // did it write something?
        if (outBBB.position() > outBB.limit()) {
            outBB.limit((int) outBBB.position());
            tryWrite();
        }
        // TODO Optimise.
        // if it read some data compact();
        if (inBBB.position() > 0) {
            inBB.position((int) inBBB.position());
            inBB.limit((int) inBBB.limit());
            inBB.compact();
            inBBB.position(0);
            inBBB.limit(inBB.position());
        }
    }

    @Override
    public boolean isDead() {
        return !sc.isOpen();
    }

    void handleIOE(IOException e) {
        e.printStackTrace();
        closeSC();
    }

    private void closeSC() {
        try {
            sc.close();
        } catch (IOException ignored) {
        }
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

    boolean tryWrite() throws IOException {
        int wrote = sc.write(outBB);
        if (wrote < 0) {
            closeSC();
        } else if (wrote > 0) {
            outBB.compact().flip();
            outBBB.position(outBB.limit());
            outBBB.limit(outBB.capacity());
            return true;
        }
        return false;
    }
}
