package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public class VanillaWireOutPublisher implements WireOutPublisher {
    private static final int WARN_QUEUE_LENGTH = 50;
    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private final Queue<WriteMarshallable> publisher = new LinkedTransferQueue<>();
    private volatile boolean closed;

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param out buffer to write to.
     */
    @Override
    public void applyAction(@NotNull WireOut out, @NotNull Runnable runnable) {
        if (publisher.isEmpty()) {
            synchronized (this) {
                runnable.run();
            }
        }
        while (out.bytes().writePosition() < out.bytes().realCapacity() / 4) {
            WriteMarshallable wireConsumer = publisher.poll();
            if (wireConsumer == null)
                break;
            wireConsumer.writeMarshallable(out);

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

        if (closed) {
            throw new IllegalStateException("Closed");

        } else {
            int size = publisher.size();
            if (size > WARN_QUEUE_LENGTH)
                LOG.debug("publish length: " + size);

            publisher.add(event);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
