package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.tcp.ISocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static net.openhft.chronicle.wire.Wires.lengthOf;

/**
 * provides flow control and back pressure to the the socket writer, so that only just one message is read at a time from the TCP/IP socket This class
 * is only used in data grid
 */
public class ReadWithFlowControl implements TcpEventHandler.SocketReader {

    int len = 0;
    boolean hasReadLen = false;
    int position = 0;
    int limit = 4;
    private int rawLen;

    /**
     * reads just a single message from the socket
     */
    public int read(ISocketChannel socketChannel, Bytes<ByteBuffer> inBBB) throws IOException {
        ByteBuffer bb = requireNonNull(inBBB.underlyingObject());
        bb.limit(limit);
        bb.position(position);
        if (hasReadLen) {
            // write the len
            inBBB.writeInt(0, rawLen);
        } else {
            // read the len
            socketChannel.read(bb);

            if (bb.position() < bb.limit())
                return 0;
            rawLen = inBBB.readInt(this.len);
            len = lengthOf(rawLen);
            inBBB.ensureCapacity(this.len + 4);
            bb = requireNonNull(inBBB.underlyingObject());
            limit(this.len + 8);
            position(4);
            limit(len + 4);
            bb.limit(limit);
            hasReadLen = true;
        }

        socketChannel.read(bb);

        // we can read the message, now read the len of the next message
        if (bb.position() == len + 4) {
            position(0);
            limit(4);
            int result = len;
            inBBB.readPositionRemaining(0, len);
            hasReadLen = false;
            len = 0;
            // read all the data from the buffer
            return result;
        }

        // we can read the message and the len
        if (bb.position() == len + 8) {
            position(4);
            int result = len;
            rawLen = inBBB.readInt(this.len);
            this.len = lengthOf(rawLen);
            limit(this.len + 4);
            inBBB.ensureCapacity(this.len + 4);
            bb = requireNonNull(inBBB.underlyingObject());
            bb.position(len + 4);
            inBBB.readPositionRemaining(0, result);
            hasReadLen = true;

            return result;
        }

        if (bb.position() > len + 4)
            throw new IllegalStateException("an error has occurred, position=" + bb.position());

        return 0;
    }

    private void limit(final int limit) {
        this.limit = limit;
    }

    private void position(final int position) {
        this.position = position;
    }

}
