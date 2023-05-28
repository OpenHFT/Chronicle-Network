/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_USE_PADDING;

public class VanillaWireOutPublisher extends AbstractCloseable implements WireOutPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private final Bytes<ByteBuffer> bytes;

    private Wire wire;
    private String connectionDescription = "?";

    public VanillaWireOutPublisher(@NotNull WireType wireType) {
        bytes = Bytes.elasticByteBuffer(TcpChannelHub.TCP_BUFFER);
        final WireType wireType0 = wireType == WireType.DELTA_BINARY ? WireType.BINARY : wireType;
        wire = wireType0.apply(bytes);
        wire.usePadding(TCP_USE_PADDING);
        bytes.singleThreadedCheckDisabled(true);
        singleThreadedCheckDisabled(true);
    }

    @Override
    public WireOutPublisher connectionDescription(String connectionDescription) {
        this.connectionDescription = connectionDescription;
        return this;
    }

    /**
     * Apply waiting messages and return <code>false</code> if there was none.
     *
     * @param bytes buffer to write to.
     */
    @Override
    public void applyAction(@NotNull Bytes<?> bytes) {
        if (this.bytes.readRemaining() > 0) {

            synchronized (lock()) {

                if (YamlLogging.showServerWrites())
                    logBuffer();

                if (bytes.writePosition() > TcpChannelHub.TCP_BUFFER)
                    return; // Write next time to prevent TCP buffer overflow.

                bytes.write(this.bytes);
                this.bytes.clear();
            }
        }
    }

    private void logBuffer() {
        long pos = this.bytes.readPosition();
        try {
            while (wire.bytes().readRemaining() > 0) {
                try (DocumentContext dc = wire.readingDocument()) {
                    Bytes<?> bytes = wire.bytes();
                    if (!dc.isPresent()) {
                        bytes.readPosition(bytes.readLimit());
                        return;
                    }
                    LOG.info("Server " + connectionDescription + " Sends async event:\n" + Wires.fromSizePrefixedBlobs(dc));
                    bytes.readPosition(bytes.readLimit());
                }
            }
        } finally {
            this.bytes.readPosition(pos);
        }
    }

    /**
     * Apply waiting messages and return <code>false</code> if there was none.
     *
     * @param outWire buffer to write to.
     */
    @Override
    public void applyAction(@NotNull WireOut outWire) {
        applyAction(outWire.bytes());
    }

    @Override
    public boolean canTakeMoreData() {
        throwExceptionIfClosed();

        synchronized (lock()) {
            throwExceptionIfClosed();

            return wire.bytes().writePosition() < TcpChannelHub.TCP_SAFE_SIZE; // don't attempt
            // to fill the buffer completely.
        }
    }

    @Override
    public void put(final Object key, @NotNull WriteMarshallable event) {
        if (logIfClosed()) return;

        // writes the data and its size
        synchronized (lock()) {
            Wire wire = this.wire;
            if (logIfClosed()) return;

            Bytes<?> bytes = wire.bytes();
            final long start = bytes.writePosition();
            event.writeMarshallable(wire);
            if (YamlLogging.showServerWrites()) {

                long rp = bytes.readPosition();
                long rl = bytes.readLimit();
                long wl = bytes.writeLimit();
                try {
                    long len = bytes.writePosition() - start;
                    bytes.readPositionRemaining(start, len);
                    String message = Wires.fromSizePrefixedBlobs(wire);
                    LOG.info("Server " + connectionDescription + " is about to send async event:" + message);
                } finally {
                    bytes.writeLimit(wl).readLimit(rl).readPosition(rp);
                }
            }
        }
    }

    private boolean logIfClosed() {
        try {
            throwExceptionIfClosed();

        } catch (ClosedIllegalStateException ise) {
            Jvm.debug().on(getClass(), "Server " + connectionDescription + " message ignored as closed", ise);
            return true;
        }
        return false;
    }

    private Object lock() {
        return this;
    }

    @Override
    protected void performClose() {
        synchronized (lock()) {
            bytes.releaseLast();
            wire = null;
        }
    }

    @Override
    public void wireType(@NotNull WireType wireType) {
        throwExceptionIfClosedInSetter();

        final WireType wireType0 = wireType == WireType.DELTA_BINARY ? WireType.BINARY : wireType;
        if (WireType.valueOf(wire) == wireType0)
            return;

        synchronized (lock()) {
            throwExceptionIfClosed();

            wire = wireType0.apply(bytes);
            wire.usePadding(TCP_USE_PADDING);
        }
    }

    @Override
    public void clear() {
        synchronized (lock()) {
            if (wire != null)
                wire.clear();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (lock()) {
            return bytes.isEmpty();
        }
    }

    @NotNull
    @Override
    public String toString() {
        String wireStr = (wire == null) ? "wire= null " : (wire.getClass().getSimpleName() + "=" + bytes);
        return "VanillaWireOutPublisher{" +
                "description=" + connectionDescription +
                ", closed=" + isClosed() +
                ", " + wireStr +
                '}';
    }
}

