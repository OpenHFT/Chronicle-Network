package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public class VanillaWireOutPublisher implements WireOutPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private Wire wire;
    private volatile boolean closed;

    public VanillaWireOutPublisher(@NotNull WireType wireType) {
        this.closed = false;
        this.wire = wireType.apply(Bytes.elasticByteBuffer(TcpChannelHub.BUFFER_SIZE));
    }

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param out buffer to write to.
     */
    @Override
    public void applyAction(@NotNull WireOut out, @NotNull Runnable read) {

        read.run();

        boolean hasReadData = false;

        synchronized (wire) {

            final Bytes<?> bytes = wire.bytes();

            if (bytes.readRemaining() < 4)
                return;

            for (; ; ) {
                final long nextElementSize = bytes.readInt(bytes.readPosition());

                if (nextElementSize == 0)
                    return;

                if (out.bytes().writeRemaining() < nextElementSize)
                    break;

                bytes.readSkip(4);
                out.bytes().write(bytes, bytes.readPosition(), nextElementSize);

                bytes.readSkip(nextElementSize);
                hasReadData = true;

                if (bytes.readRemaining() < 4)
                    break;
            }

            if (!hasReadData)
                return;

            if (bytes.readRemaining() == 0)
                bytes.clear();
            else {
                final ByteBuffer o = (ByteBuffer) bytes.underlyingObject();
                o.position(0);
                o.limit((int) bytes.readLimit());
                o.position((int) bytes.readPosition());

                o.compact();
                final int byteCoppied = o.position();

                bytes.readPosition(0);
                bytes.writePosition(byteCoppied);

            }

            if (YamlLogging.showServerWrites)
                try {
                    LOG.info("\nServer Publishes (from async publisher ) :\n" +
                            Wires.fromSizePrefixedBlobs(out.bytes()));

                } catch (Exception e) {
                    LOG.info("\nServer Publishes ( from async publisher - corrupted ) :\n" +
                            out.bytes().toDebugString());
                    LOG.error("", e);
                }
        }
    }

    @Override
    public void put(final Object key, WriteMarshallable event) {
      /*  if (wire == null) {
            final WireType wireType = this.wireType.get();

            if (wireType == null)

                System.out.println("");
            wire = wireType.apply(Bytes.elasticByteBuffer(TcpChannelHub.BUFFER_SIZE));
        }
*/

        // writes the data and its size
        synchronized (wire) {
            final Bytes<?> bytes = wire.bytes();
            final long sizePosition = bytes.writePosition();
            bytes.writeSkip(4);
            final long start = bytes.writePosition();

            try {
                event.writeMarshallable(wire);
                final int size = (int) (bytes.writePosition() - start);
                bytes.writeInt(sizePosition, size);

                //     System.out.println("PUBLISHER -> size=" + size + "\n" +
                //            Wires.fromSizePrefixedBlobs(bytes, start, size));
            } catch (Exception e) {
                bytes.writePosition(start - 4);
                throw e;
            }
        }


    }

    @Override
    public boolean isClosed() {
        return closed;
    }


    @Override
    public synchronized void close() {
        closed = true;
        synchronized (wire) {
            wire.clear();
        }
    }

    public boolean canTakeMoreData() {
        return wire.bytes().writePosition() < TcpChannelHub.BUFFER_SIZE;
    }
}

