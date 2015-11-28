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

        if (wire.bytes().readRemaining() == 0)
            return;

        out.bytes().write(wire.bytes());
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
        System.out.println("pos=" + wire.bytes().writePosition());
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
