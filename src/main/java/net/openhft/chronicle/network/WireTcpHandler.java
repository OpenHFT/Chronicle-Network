/*
 * Copyright 2016-2020 http://chronicle.software
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.wire.WireType.BINARY;
import static net.openhft.chronicle.wire.WireType.DELTA_BINARY;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

public abstract class WireTcpHandler<T extends NetworkContext<T>>
        extends SimpleCloseable
        implements TcpHandler<T>, NetworkContextManager<T> {

    private static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(WireTcpHandler.class);
    // this is the point at which it is worth doing more work to get more data.

    protected Wire outWire;
    long lastWritePosition = 0;
    long writeBps;
    long bytesReadCount;
    int socketPollCount;
    volatile long lastMonitor;
    private Wire inWire;
    private boolean recreateWire;
    @Nullable
    protected WireType wireType;
    private WireOutPublisher publisher;
    private T nc;
    private boolean isAcceptor;
    private long lastReadRemaining;

    private static void logYaml(@NotNull final DocumentContext dc) {
        if (YamlLogging.showServerWrites() || YamlLogging.showServerReads())
            try {
                LOG.info("\nDocumentContext:\n" +
                        Wires.fromSizePrefixedBlobs(dc));

            } catch (Exception e) {
                Jvm.warn().on(WireOutPublisher.class, "\nServer Sends ( corrupted ) :\n" +
                        dc.wire().bytes().toDebugString());
            }
    }

    private static void logYaml(@NotNull final WireOut outWire) {
        if (YamlLogging.showServerWrites())
            try {
                LOG.info("\nServer Sends:\n" +
                        Wires.fromSizePrefixedBlobs((Wire) outWire));

            } catch (Exception e) {
                Jvm.warn().on(WireOutPublisher.class, "\nServer Sends ( corrupted ) :\n" +
                        outWire.bytes().toDebugString());
            }
    }

    public boolean isAcceptor() {
        return this.isAcceptor;
    }

    public void wireType(@NotNull WireType wireType) {
        if (wireType == BINARY)
            wireType = DELTA_BINARY.isAvailable() ? DELTA_BINARY : BINARY;
        this.wireType = wireType;

        if (publisher != null)
            publisher.wireType(wireType);
    }

    public WireOutPublisher publisher() {
        return publisher;
    }

    public void publisher(@NotNull final WireOutPublisher publisher) {
        this.publisher = publisher;
        if (wireType() != null)
            publisher.wireType(wireType());
    }

    public void isAcceptor(final boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, final T nc) {

        if (isClosed())
            return;

        WireType wireType = wireType();
        if (wireType == null)
            wireType = in.readByte(in.readPosition() + 4) < 0 ? WireType.BINARY : WireType.TEXT;

        checkWires(in, out, wireType);

        // we assume that if any bytes were in lastOutBytesRemaining the sc.write() would have been
        // called and this will fail, if the other end has lost its connection
        if (outWire.bytes().writePosition() != lastWritePosition) {
            onBytesWritten();

            // NOTE you can not use remaining as the buffer maybe resized
            writeBps += (lastWritePosition - outWire.bytes().writePosition());
        }

        socketPollCount++;

        bytesReadCount += (in.readRemaining() - lastReadRemaining);

        final long now = System.currentTimeMillis();
        if (now > lastMonitor + 10000) {
            final NetworkStatsListener<T> networkStatsListener = this.nc.networkStatsListener();

            if (networkStatsListener != null && !networkStatsListener.isClosed()) {
                if (lastMonitor == 0) {
                    networkStatsListener.onNetworkStats(0, 0, 0);
                } else {
                    networkStatsListener.onNetworkStats(writeBps / 10, bytesReadCount / 10,
                            socketPollCount / 10);

                    writeBps = bytesReadCount = socketPollCount = 0;
                }
            }
            lastMonitor = now;
        }

        if (publisher != null)
            publisher.applyAction(outWire);

        if (in.readRemaining() >= SIZE_OF_SIZE)
            onRead0();
        else
            onWrite(outWire);

        lastWritePosition = outWire.bytes().writePosition();
        lastReadRemaining = inWire.bytes().readRemaining();

    }

    protected void onBytesWritten() {
    }

    @Override
    public void onEndOfConnection(final boolean heartbeatTimeOut) {
        // throwExceptionIfClosed(); - knows we might be shutting down.

        final NetworkStatsListener<T> networkStatsListener = this.nc.networkStatsListener();

        if (networkStatsListener != null)
            networkStatsListener.onNetworkStats(-1, -1, -1);

        if (publisher != null)
            publisher.close();
    }

    protected void onWrite(@NotNull final WireOut out) {
    }

    /**
     * process all messages in this batch, provided there is plenty of output space.
     */
    private void onRead0() {
        assert inWire.startUse();

        ensureCapacity();

        try {

            while (!inWire.bytes().isEmpty()) {

                try (DocumentContext dc = inWire.readingDocument()) {
                    if (!dc.isPresent())
                        return;

                    try {
                        if (YamlLogging.showServerReads())
                            logYaml(dc);
                        onRead(dc, outWire);
                        onWrite(outWire);
                    } catch (Exception e) {
                        Jvm.warn().on(getClass(), "inWire=" + inWire.getClass() + ",yaml=" + Wires.fromSizePrefixedBlobs(dc), e);
                    }
                }
            }
        } finally {
            assert inWire.endUse();
        }
    }

    private void ensureCapacity() {
        @NotNull final Bytes<?> bytes = inWire.bytes();
        if (bytes.readRemaining() >= 4) {
            final long pos = bytes.readPosition();
            int length = bytes.readInt(pos);
            final long size = pos + Wires.SPB_HEADER_SIZE * 2 + Wires.lengthOf(length);
            if (size > bytes.realCapacity()) {
                resizeInWire(size);
            }
        }
    }

    private void resizeInWire(final long size) {
        @NotNull final Bytes<?> bytes = inWire.bytes();
        if (size > bytes.realCapacity()) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), Integer.toHexString(System.identityHashCode(bytes)) + " resized to: " + size);
            bytes.ensureCapacity(size);
        }
    }

    protected void checkWires(final Bytes<?> in, Bytes<?> out, @NotNull final WireType wireType) {
        if (recreateWire) {
            recreateWire = false;
            initialiseInWire(wireType, in);
            initialiseOutWire(out, wireType);
            return;
        }

        if (inWire == null) {
            initialiseInWire(wireType, in);
            recreateWire = false;
        }

        assert inWire.startUse();
        if (inWire.bytes() != in) {
            initialiseInWire(wireType, in);
            recreateWire = false;
        }
        assert inWire.endUse();

        boolean replace = outWire == null;
        if (!replace) {
            assert outWire.startUse();
            replace = outWire.bytes() != out;
            assert outWire.endUse();
        }
        if (replace) {
            initialiseOutWire(out, wireType);
            recreateWire = false;
        }
    }

    protected Wire initialiseOutWire(final Bytes<?> out, @NotNull final WireType wireType) {
        return outWire = wireType.apply(out);
    }

    protected Wire initialiseInWire(@NotNull final WireType wireType, final Bytes<?> in) {
        return inWire = wireType.apply(in);
    }

    /**
     * Process an incoming request
     */
    public WireType wireType() {
        return this.wireType;
    }

    /**
     * @param in  the wire to be processed
     * @param out the result of processing the {@code in}
     */
    protected abstract void onRead(@NotNull DocumentContext in,
                                   @NotNull WireOut out);

    /**
     * write and exceptions and rolls back if no data was written
     */
    protected void writeData(@NotNull final Bytes<?> inBytes, @NotNull final WriteMarshallable c) {
        outWire.writeDocument(false, out -> {
            final long readPosition = inBytes.readPosition();
            final long position = outWire.bytes().writePosition();
            try {
                c.writeMarshallable(outWire);
            } catch (Throwable t) {
                inBytes.readPosition(readPosition);
                if (LOG.isInfoEnabled())
                    LOG.info("While reading " + inBytes.toDebugString(),
                            " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(t);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        });

        logYaml(outWire);
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    protected void writeData(final boolean isNotComplete,
                             @NotNull final Bytes<?> inBytes,
                             @NotNull final WriteMarshallable c) {

        @NotNull final WriteMarshallable marshallable = out -> {
            final long readPosition = inBytes.readPosition();
            final long position = outWire.bytes().writePosition();
            try {
                c.writeMarshallable(outWire);
            } catch (Throwable t) {
                inBytes.readPosition(readPosition);
                if (LOG.isInfoEnabled())
                    LOG.info("While reading " + inBytes.toDebugString(),
                            " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(t);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        };

        if (isNotComplete)
            outWire.writeNotCompleteDocument(false, marshallable);
        else
            outWire.writeDocument(false, marshallable);

        logYaml(outWire);
    }

    @Override
    public final void nc(final T nc) {
        this.nc = nc;
        if (!isClosed())
            onInitialize();
    }

    @Override
    public T nc() {
        return nc;
    }

    protected abstract void onInitialize();

    @Override
    protected void performClose() {
        Closeable.closeQuietly(publisher,nc);
    }

    protected void publish(final WriteMarshallable w) {
        publisher.put("", w);
    }
}
