package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public class VanillaWireOutPublisher implements WireOutPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private volatile boolean closed;
    private final Wire wire;

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
    public synchronized void applyAction(@NotNull WireOut out, @NotNull Runnable runnable) {

        if (isEmpty()) {
            synchronized (this) {
                runnable.run();
            }

        }


        final long sourceBytesRemaining = wire.bytes().readRemaining();


        if (sourceBytesRemaining == 0)
            return;

        if (out.bytes().writeRemaining() < sourceBytesRemaining)
            return;

        final long targetBytesRemaining = out.bytes().writeRemaining();
        out.bytes().write(wire.bytes());

        final long bytesWritten = targetBytesRemaining - out.bytes().writeRemaining();

        assert bytesWritten == sourceBytesRemaining : "bytesWritten=" + bytesWritten + ", " +
                "sourceBytesRemaining=" + sourceBytesRemaining;

        wire.bytes().clear();

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

    @Override
    public synchronized void put(final Object key, WriteMarshallable event) {
        assert isEmpty();
        event.writeMarshallable(wire);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized boolean isEmpty() {
        return wire.bytes().writePosition() == 0;
    }

    @Override
    public synchronized void close() {
        closed = true;
        wire.clear();
    }
}
