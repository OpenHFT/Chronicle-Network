/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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
    public String toString() {


        return "VanillaWireOutPublisher{" +
                ", closed=" + closed +
                ", " + wire.getClass().getSimpleName() + "=" + bytes +
                '}';
    }
}

