package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public class VanillaWireOutPublisher implements WireOutPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private final Wire wire;
    private volatile boolean closed;

    public VanillaWireOutPublisher(@NotNull Function<Bytes, Wire> wireType) {
        this.closed = false;
        this.wire = wireType.apply(Bytes.elasticByteBuffer());
    }

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param out buffer to write to.
     */
    @Override
    public synchronized void applyAction(@NotNull WireOut out, @NotNull Runnable read) {

//        if (isEmpty()) {
        read.run();
//        }
        boolean hasReadData = false;

        final Bytes<?> bytes = wire.bytes();
        synchronized (wire) {
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
        final Bytes<?> bytes = wire.bytes();
        // writes the data and its size
        synchronized (wire) {
            final long sizePosition = bytes.writePosition();
            bytes.writeSkip(4);
            final long start = bytes.writePosition();
            event.writeMarshallable(wire);
            final int size = (int) (bytes.writePosition() - start);
            bytes.writeInt(sizePosition, size);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isEmpty() {
        assert Thread.holdsLock(this);
        synchronized (wire) {
            return wire.bytes().writePosition() == 0;
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        synchronized (wire) {
            wire.clear();
        }
    }
}

