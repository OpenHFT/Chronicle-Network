/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private final Bytes<ByteBuffer> bytes;
    private Wire wrapperWire;
    private volatile boolean closed;
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

        if (bytes.readRemaining() == 0)
            return;

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
    public void wireType(@NotNull WireType wireType) {
        if (WireType.valueOf(wire) == wireType)
            return;

        synchronized (lock()) {
            wire = wireType.apply(bytes);
        }
    }

    @Override
    public void clear() {
        synchronized (lock()) {
            wrapperWire.clear();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (lock()) {
            return bytes.isEmpty();
        }
    }

    @Override
    public String toString() {


        return "VanillaWireOutPublisher{" +
                ", closed=" + closed +
                ", " + wire.getClass().getSimpleName() + "=" + bytes +
                '}';
    }
}

