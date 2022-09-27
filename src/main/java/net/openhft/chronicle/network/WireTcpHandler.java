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

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_USE_PADDING;
import static net.openhft.chronicle.wire.WireType.BINARY;
import static net.openhft.chronicle.wire.WireType.DELTA_BINARY;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

public abstract class WireTcpHandler<T extends NetworkContext<T>>
        extends SimpleCloseable
        implements TcpHandler<T>, NetworkContextManager<T> {

    private static final int SIZE_OF_SIZE = 4;
    // this is the point at which it is worth doing more work to get more data.
    protected Wire outWire;
    @Nullable
    protected WireType wireType;
    long lastWritePosition = 0;
    private Wire inWire;
    private WireOutPublisher publisher;
    private T nc;
    private boolean isAcceptor;

    private static void logYaml(@NotNull final DocumentContext dc) {
        try {
            Jvm.startup().on(WireTcpHandler.class,
                    "Server Reads:\n" +
                            Wires.fromSizePrefixedBlobs(dc));

        } catch (Exception e) {
            Jvm.warn().on(WireOutPublisher.class, "\nServer Reads ( corrupted ) :\n" +
                    dc.wire().bytes().toDebugString());
        }
    }

    private static void logYaml(@NotNull final WireOut outWire) {
        if (YamlLogging.showServerWrites())
            try {
                Jvm.startup().on(WireTcpHandler.class,
                        "Server Sends:\n" +
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
    public void process(@NotNull final Bytes<?> in, @NotNull final Bytes<?> out, final T nc) {

        if (isClosed())
            return;

        WireType wireType = wireType();
        if (wireType == null)
            wireType = in.readByte(in.readPosition() + 4) < 0 ? WireType.BINARY : WireType.TEXT;

        checkWires(in, out, wireType);

        // we assume that if any bytes were in lastOutBytesRemaining the sc.write() would have been
        // called and this will fail, if the other end has lost its connection
        Bytes<?> bytes = outWire.bytes();
        if (bytes.writePosition() != lastWritePosition)
            onBytesWritten();

        if (publisher != null)
            publisher.applyAction(outWire);

        if (in.readRemaining() >= SIZE_OF_SIZE)
            onRead0();
        else {
            long remaining = bytes.readRemaining();
            if (YamlLogging.showServerWrites() && remaining >= 4) {
                int length = Wires.lengthOf(bytes.peekVolatileInt());
                if (length <= remaining)
                    Jvm.startup().on(WireTcpHandler.class, "sending from WTH: " + Wires.fromSizePrefixedBlobs(outWire));
                else
                    Jvm.startup().on(WireTcpHandler.class, "send remaining from WTH: " + remaining);
            }
            onWrite(outWire);
        }

        lastWritePosition = bytes.writePosition();
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
        ensureCapacity();

        long index = -1;
        while (!inWire.bytes().isEmpty()) {

            try (DocumentContext dc = inWire.readingDocument()) {
                if (!dc.isPresent())
                    return;

                // if the last iteration didn't consume anything (rolled back) then break
                if (dc.index() == index) {
                    dc.rollbackOnClose();
                    return;
                }
                index = dc.index();

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
        if (inWire == null) {
            initialiseInWire(wireType, in);
        }

        if (inWire.bytes() != in) {
            initialiseInWire(wireType, in);
        }

        boolean replace = outWire == null;
        if (!replace) {
            replace = outWire.bytes() != out;
        }
        if (replace) {
            initialiseOutWire(out, wireType);
        }
    }

    protected Wire initialiseOutWire(final Bytes<?> out, @NotNull final WireType wireType) {
        final Wire wire = wireType.apply(out);
        wire.usePadding(TCP_USE_PADDING);
        return outWire = wire;
    }

    protected Wire initialiseInWire(@NotNull final WireType wireType, final Bytes<?> in) {
        final Wire wire = wireType.apply(in);
        wire.usePadding(TCP_USE_PADDING);
        return inWire = wire;
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
                Jvm.warn().on(WireTcpHandler.class,
                        "While reading " + inBytes.toDebugString() + " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName("exception").throwable(t);
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
                Jvm.warn().on(WireTcpHandler.class,
                        "While reading " + inBytes.toDebugString() + " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName("exception").throwable(t);
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
        Closeable.closeQuietly(publisher, nc);
    }

    protected void publish(final WriteMarshallable w) {
        publisher.put("", w);
    }
}
