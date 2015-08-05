/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.util.function.Function;

public abstract class WireTcpHandler implements TcpHandler {
    private static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(WireTcpHandler.class);
    // this is the point at which it is worth doing more work to get more data.
    private static final int SMALL_WRITE_BUFFER = Integer.getInteger("WireTcpHandler.SMALL_WRITE_BUFFER", 32 << 10);
    @NotNull
    private final Function<Bytes, Wire> bytesToWire;
    private Wire inWire;
    protected Wire outWire;
    private boolean recreateWire;
    protected final WireOutPublisher publisher = new WireOutPublisher();

    public WireTcpHandler(@NotNull final Function<Bytes, Wire> bytesToWire) {
        this.bytesToWire = bytesToWire;
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out, @NotNull SessionDetailsProvider sessionDetails) {
        checkWires(in, out);

        publisher.applyAction(outWire, () -> {
            if (in.readRemaining() >= SIZE_OF_SIZE && out.writePosition() < SMALL_WRITE_BUFFER)
                read(in, out, sessionDetails);
        });
    }

    public void sendHeartBeat(Bytes out, SessionDetailsProvider sessionDetails) {
        final WireOut outWire = bytesToWire.apply(out);
        outWire.writeDocument(true, w -> w.write(() -> "tid").int64(0));
        outWire.writeDocument(false, w -> w.writeEventName(() -> "heartbeat").int64(Time.currentTimeMillis()));
    }

    @Override
    public void onEndOfConnection(boolean heartbeatTimeOut) {
        publisher.close();
    }

    /**
     * process all messages in this batch, provided there is plenty of output space.
     *
     * @param in  the source bytes
     * @param out the destination bytes
     * @return true if we can read attempt the next
     */
    private boolean read(@NotNull Bytes in, @NotNull Bytes out, @NotNull SessionDetailsProvider sessionDetails) {
        final long header = in.readInt(in.readPosition());
        long length = Wires.lengthOf(header);
        assert length >= 0 && length < 1 << 23 : "in=" + in + ", hex=" + in.toHexString();

        // we don't return on meta data of zero bytes as this is a system message
        if (length == 0 && Wires.isData(header)) {
            in.readSkip(SIZE_OF_SIZE);
            return false;
        }

        if (in.readRemaining() < length) {
            // we have to first read more data before this can be processed
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("required length=%d but only got %d bytes, " +
                                "this is short by %d bytes", length, in.readRemaining(),
                        length - in.readRemaining()));
            return false;
        }

        long limit = in.readLimit();
        long end = in.readPosition() + length + SIZE_OF_SIZE;
        assert end <= limit;
        long outPos = out.writePosition();
        try {

            in.readLimit(end);

            final long position = inWire.bytes().readPosition();
            try {
                process(inWire, outWire, sessionDetails);
            } finally {
                try {
                    inWire.bytes().readPosition(position + length);
                } catch (BufferOverflowException e) {
                    //noinspection ThrowFromFinallyBlock
                    throw new IllegalStateException("Unexpected error position: " + position + ", length: " + length + " limit(): " + inWire.bytes().readLimit(), e);
                }
            }

            long written = out.writePosition() - outPos;

            if (written > 0)
                return false;
        } catch (Throwable e) {
            LOG.error("", e);
        } finally {
            in.readLimit(limit);
            try {
                in.readPosition(end);
            } catch (Exception e) {
                throw new IllegalStateException("position: " + end
                        + ", limit:" + limit + ", readLimit: " + in.readLimit() + " " + in.toDebugString(), e);

            }
        }

        return true;
    }

    private void checkWires(Bytes in, Bytes out) {
        if (recreateWire) {
            recreateWire = false;
            inWire = bytesToWire.apply(in);
            outWire = bytesToWire.apply(out);
            return;
        }

        if ((inWire == null || inWire.bytes() != in)) {
            inWire = bytesToWire.apply(in);
            recreateWire = false;
        }

        if ((outWire == null || outWire.bytes() != out)) {
            outWire = bytesToWire.apply(out);
            recreateWire = false;
        }
    }

    /**
     * Process an incoming request
     */

    /**
     * @param in  the wire to be processed
     * @param out the result of processing the {@code in}
     * @throws StreamCorruptedException if the wire is corrupt
     */
    protected abstract void process(@NotNull WireIn in,
                                    @NotNull WireOut out,
                                    @NotNull SessionDetailsProvider sessionDetails)
            throws StreamCorruptedException;

}
