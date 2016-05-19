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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

public abstract class WireTcpHandler<T extends NetworkContext> implements TcpHandler,
        NetworkContextManager<T> {

    private static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(WireTcpHandler.class);
    // this is the point at which it is worth doing more work to get more data.

    @NotNull
    protected Wire outWire;
    long lastOutBytesWriteRemaining = 0;
    int onBytesWrittenCount, noByteWrittenCount;
    long lastMonitor = System.currentTimeMillis();
    @NotNull
    private Wire inWire;
    private boolean recreateWire;
    @Nullable
    private WireType wireType;
    private WireOutPublisher publisher;
    private T nc;
    private volatile boolean closed;
    private boolean isAcceptor;

    private static void logYaml(final WireOut outWire) {
        if (YamlLogging.showServerWrites())
            try {
                LOG.info("\nServer Sends:\n" +
                        Wires.fromSizePrefixedBlobs(outWire.bytes()));
            } catch (Exception e) {
                LOG.info("\nServer Sends ( corrupted ) :\n" +
                        outWire.bytes().toDebugString());
            }
    }

    public boolean isAcceptor() {
        return this.isAcceptor;
    }

    public void wireType(@NotNull WireType wireType) {
        this.wireType = wireType;
        if (publisher != null)
            publisher.wireType(wireType);
    }

    public WireOutPublisher publisher() {
        return publisher;
    }

    public void publisher(WireOutPublisher publisher) {
        this.publisher = publisher;
        if (wireType() != null)
            publisher.wireType(wireType());
    }

    public void isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out) {

        if (closed)
            return;

        WireType wireType = wireType();
        if (wireType == null)
            wireType = in.readByte(in.readPosition() + 4) < 0 ? WireType.BINARY : WireType.TEXT;
        checkWires(in, out, wireType);

        // we assume that if any bytes were in lastOutBytesRemaining the sc.write() would have been
        // called and this will fail, if the other end has lost its connection
        if (outWire.bytes().writeRemaining() != lastOutBytesWriteRemaining) {
            onBytesWritten();
            onBytesWrittenCount++;
        } else {
            noByteWrittenCount++;
        }
        long now = System.currentTimeMillis();
        if (now > lastMonitor + 1000) {
            lastMonitor = now;
            System.out.println(this + " onBytes: " + onBytesWrittenCount + " noBytes: " + noByteWrittenCount);
            onBytesWrittenCount = noByteWrittenCount = 0;
        }

        if (publisher != null && out.writePosition() < TcpEventHandler.TCP_BUFFER)
            publisher.applyAction(outWire);

        if (in.readRemaining() >= SIZE_OF_SIZE)
            onRead0();

        if (out.writePosition() < TcpEventHandler.TCP_BUFFER)
            onWrite(outWire);

        lastOutBytesWriteRemaining = outWire.bytes().writeRemaining();
    }

    protected void onBytesWritten() {

    }

    @Override
    public void onEndOfConnection(boolean heartbeatTimeOut) {
        if (publisher != null)
            publisher.close();
    }

    protected void onWrite(@NotNull WireOut out) {

    }

    /**
     * process all messages in this batch, provided there is plenty of output space.
     */
    private void onRead0() {
        assert inWire.startUse();

        try {
            while (!inWire.bytes().isEmpty()) {
                long start = inWire.bytes().readPosition();
                try (DocumentContext dc = inWire.readingDocument()) {
                    if (!dc.isPresent()) {
                        return;
                    }

                    try {
                        logYaml(start);
                        onRead(dc, outWire);

                    } catch (Exception e) {
                        LOG.error("inWire=" + inWire.getClass(), e);
                    }
                }
            }
        } finally {
            assert inWire.endUse();
        }
    }

    private void logYaml(long start) {
        if (YamlLogging.showServerReads() && !inWire.bytes().isEmpty()) {
            String s = Wires.fromSizePrefixedBlobs(inWire.bytes(), start, inWire.bytes()
                    .readLimit());
            LOG.info("handler=" + this.getClass().getSimpleName() + ", read:\n" + s);
        }
    }

    protected void checkWires(Bytes in, Bytes out, @NotNull WireType wireType) {
        if (recreateWire) {
            recreateWire = false;
            inWire = wireType.apply(in);
            outWire = wireType.apply(out);
            return;
        }

        if (inWire == null) {
            inWire = wireType.apply(in);
            recreateWire = false;
        }

        assert inWire.startUse();
        if (inWire.bytes() != in) {
            inWire = wireType.apply(in);
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
            outWire = wireType.apply(out);
            recreateWire = false;
        }
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
    protected void writeData(@NotNull Bytes inBytes, @NotNull WriteMarshallable c) {
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
    protected void writeData(boolean isNotComplete, @NotNull Bytes inBytes, @NotNull WriteMarshallable c) {

        final WriteMarshallable marshallable = out -> {
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

    public final void nc(T nc) {
        this.nc = nc;
        onInitialize();
    }

    public T nc() {
        return nc;
    }

    protected abstract void onInitialize();

    @Override
    public void close() {
        closed = true;
        nc.connectionClosed(true);
    }

    protected void publish(WriteMarshallable w) {
        publisher.put("", w);
    }
}
