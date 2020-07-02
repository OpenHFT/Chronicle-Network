/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
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
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class VanillaWireOutPublisher extends AbstractCloseable implements WireOutPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaWireOutPublisher.class);
    private final Bytes<ByteBuffer> bytes;

    private Wire wire;
    @NotNull
    private List<WireOutConsumer> consumers = new CopyOnWriteArrayList<>();
    private int consumerIndex;

    public VanillaWireOutPublisher(@NotNull WireType wireType) {
        bytes = Bytes.elasticByteBuffer(TcpChannelHub.TCP_BUFFER);
        final WireType wireType0 = wireType == WireType.DELTA_BINARY ? WireType.BINARY : wireType;
        wire = wireType0.apply(bytes);
    }

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param bytes buffer to write to.
     */
    @Override
    public void applyAction(@NotNull Bytes bytes) {
        if (this.bytes.readRemaining() > 0) {

            synchronized (lock()) {

                if (YamlLogging.showServerWrites())
                    logBuffer();

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
                    LOG.info("Server Sends async event:\n" + Wires.fromSizePrefixedBlobs(dc));
                    bytes.readPosition(bytes.readLimit());
                }
            }
        } finally {
            this.bytes.readPosition(pos);
        }
    }

    /**
     * Apply waiting messages and return false if there was none.
     *
     * @param outWire buffer to write to.
     */
    @Override
    public void applyAction(@NotNull WireOut outWire) {
        throwExceptionIfClosed();

        applyAction(outWire.bytes());

        for (int y = 1; y < 1000; y++) {

            long pos = outWire.bytes().writePosition();

            for (int i = 0; i < consumers.size(); i++) {

                if (outWire.bytes().writePosition() >= TcpChannelHub.TCP_SAFE_SIZE)
                    return;

                if (isClosed())
                    return;

                WireOutConsumer c = next();

                try {
                    c.accept(outWire);
                } catch (InvalidEventHandlerException e) {
                    consumers.remove(c);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Jvm.warn().on(getClass(), e);
                    return;
                }
            }

            if (pos == outWire.bytes().writePosition())
                return;
            if (y >= 100)
                Jvm.pause(y / 100);
        }

        Jvm.warn().on(getClass(), new IllegalStateException("looped for too long"));

    }

    @Override
    public void addWireConsumer(WireOutConsumer wireOutConsumer) {
        throwExceptionIfClosedInSetter();

        consumers.add(wireOutConsumer);
    }

    @Override
    public boolean removeBytesConsumer(WireOutConsumer wireOutConsumer) {
        throwExceptionIfClosedInSetter();

        return consumers.remove(wireOutConsumer);
    }

    /**
     * round robins - the consumers, we should only write when the buffer is empty, as we can't
     * guarantee that we will have enough space to add more data to the out wire.
     *
     * @return the  Marshallable that you are writing to
     */
    private WireOutConsumer next() {
        if (consumerIndex >= consumers.size())
            consumerIndex = 0;
        return consumers.get(consumerIndex++);
    }

    @Override
    public void put(final Object key, @NotNull WriteMarshallable event) {
        try {
            throwExceptionIfClosed();

        } catch (IllegalStateException ise) {
            Jvm.debug().on(getClass(), "message ignored as closed", ise);
            return;
        }

        // writes the data and its size
        synchronized (lock()) {
            assert wire.startUse();
            try {
                final long start = wire.bytes().writePosition();
                event.writeMarshallable(wire);
                if (YamlLogging.showServerWrites()) {

                    long rp = wire.bytes().readPosition();
                    long rl = wire.bytes().readLimit();
                    long wl = wire.bytes().writeLimit();
                    try {
                        long len = wire.bytes().writePosition() - start;
                        wire.bytes().readPositionRemaining(start, len);
                        String message = Wires.fromSizePrefixedBlobs(wire);
                        LOG.info("Server is about to send async event:" + message);
                    } finally {
                        wire.bytes().writeLimit(wl).readLimit(rl).readPosition(rp);
                    }
                }
            } finally {
                assert wire.endUse();
            }
        }
    }

    private Object lock() {
        return this;
    }

    @Override
    protected void performClose() {
        bytes.releaseLast();
        clear();
    }

    @Override
    public void wireType(@NotNull WireType wireType) {
        throwExceptionIfClosedInSetter();

        final WireType wireType0 = wireType == WireType.DELTA_BINARY ? WireType.BINARY : wireType;
        if (WireType.valueOf(wire) == wireType0)
            return;

        synchronized (lock()) {
            wire = wireType0.apply(bytes);
        }
    }

    @Override
    public void clear() {
        synchronized (lock()) {
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
        return "VanillaWireOutPublisher{" +
                ", closed=" + isClosed() +
                ", " + wire.getClass().getSimpleName() + "=" + bytes +
                '}';
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        // assume thread safe
        return true;
    }
}

