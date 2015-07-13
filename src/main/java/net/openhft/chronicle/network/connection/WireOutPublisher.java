package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.WireOut;

import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public class WireOutPublisher implements Closeable {
    private static final int WARN_QUEUE_LENGTH = 50;
    private final Queue<Consumer<WireOut>> publisher = new LinkedTransferQueue<>();
    private WireOut out;
    private volatile boolean closed;

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param out buffer to write to.
     */
    public void applyAction(WireOut out, Runnable runnable) {
        if (publisher.isEmpty()) {
            synchronized (this) {
                try {
                    this.out = out;
                    runnable.run();

                } finally {
                    this.out = null;
                }
            }
        }
        while (out.bytes().writePosition() < out.bytes().realCapacity() / 4) {
            Consumer<WireOut> wireConsumer = publisher.poll();
            if (wireConsumer == null)
                break;
            wireConsumer.accept(out);
        }
    }

    public void add(Consumer<WireOut> outConsumer) {
        if (Thread.holdsLock(this)) {
            outConsumer.accept(out);

        } else if (closed) {
            throw new IllegalStateException("Closed");

        } else {
            int size = publisher.size();
            if (size > WARN_QUEUE_LENGTH)
                System.out.println("publish length: " + size);

            publisher.add(outConsumer);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
