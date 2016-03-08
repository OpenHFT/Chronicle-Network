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

    private Wire wrapperWire;
    private volatile boolean closed;
    private final Bytes<ByteBuffer> bytes;
    private Wire wire;

    public VanillaWireOutPublisher(WireType wireType) {
        this.closed = false;
        bytes = Bytes.elasticByteBuffer(TcpChannelHub.BUFFER_SIZE);
        wrapperWire = WireType.BINARY.apply(bytes);
        wire = wireType.apply(bytes);
    }

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param out buffer to write to.
     */
    @Override
    public void applyAction(@NotNull Bytes out) {

        synchronized (lock()) {

            while (bytes.readRemaining() > 0) {

                final long readPosition = bytes.readPosition();
                try (final ReadDocumentContext dc = (ReadDocumentContext) wrapperWire.readingDocument()) {

                    if (!dc.isPresent() || out.writeRemaining() < bytes.readRemaining()) {
                        dc.closeReadPosition(readPosition);
                        return;
                    }

                    if (YamlLogging.showServerWrites)
                        LOG.info("Server sends:" + Wires.fromSizePrefixedBlobs(bytes));

                    out.write(bytes);

                }
            }

            bytes.compact();

        }
    }

    @Override
    public void put(final Object key, WriteMarshallable event) {

        if (closed) {
            LOG.debug("message ignored as closed");
            return;
        }

        // writes the data and its size
        synchronized (lock()) {
            wrapperWire.writeDocument(false, d -> {

                final long start = wire.bytes().writePosition();
                event.writeMarshallable(wire);
                if (YamlLogging.showServerWrites)
                    LOG.info("Server is about to send:" + Wires.fromSizePrefixedBlobs(wire.bytes(),
                            start, wire
                                    .bytes().writePosition() - start));

            });
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }


    private Object lock() {
        return bytes;
    }

    @Override
    public synchronized void close() {
        closed = true;
        synchronized (lock()) {
            wrapperWire.clear();
        }
    }

    public boolean canTakeMoreData() {
        return wrapperWire.bytes().writePosition() < TcpChannelHub.BUFFER_SIZE;
    }

    @Override
    public String toString() {
        return Wires.fromSizePrefixedBlobs(bytes);
    }


    @Override
    public void wireType(WireType wireType) {
        synchronized (lock()) {
            wire = wireType.apply(bytes);
        }
    }
}

