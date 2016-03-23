/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class HeaderTcpHandler<T extends NetworkContext> implements TcpHandler {

    public static final String HANDLER = "handler";
    private static final Logger LOG = LoggerFactory.getLogger(HeaderTcpHandler.class);
    private final TcpEventHandler handlerManager;
    private final Function<Object, TcpHandler> handlerFunction;
    private final NetworkContext nc;


    public HeaderTcpHandler(@NotNull final TcpEventHandler handlerManager,
                            @NotNull final Function<Object, TcpHandler> handlerFunction,
                            @NotNull final T nc) {
        this.handlerManager = handlerManager;
        this.handlerFunction = handlerFunction;
        this.nc = nc;
    }

    public static WriteMarshallable toHeader(final WriteMarshallable m) {
        return wire -> {
            try (final DocumentContext dc = wire.writingDocument(false)) {
                wire.write(() -> HANDLER).typedMarshallable(m);
            }
        };
    }

    public static WriteMarshallable toHeader(final WriteMarshallable m, byte localIdentifier, byte remoteIdentifier) {
        return wire -> {
            try (final DocumentContext dc = wire.writingDocument(false)) {
                wire.write(() -> HANDLER).typedMarshallable(m);
                wire.writeComment("client:localIdentifier=" + localIdentifier + ", " +
                        "remoteIdentifier=" + remoteIdentifier);
            }
        };
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out) {

        assert nc.wireType() != null;

        // the type of the header
        final Wire inWire = nc.wireType().apply(in);

        long start = in.readPosition();

        try (final DocumentContext dc = inWire.readingDocument()) {

            if (!dc.isPresent())
                return;

            if (YamlLogging.showServerReads)
                LOG.info("read:\n" + Wires.fromSizePrefixedBlobs(in, start, in.readLimit() - start));

            if (!dc.isData())
                throw new IllegalStateException("expecting a header of type data.");


            if (!dc.isData())
                throw new IllegalStateException("expecting a header of type data.");

            final TcpHandler handler;

            final long readPosition = inWire.bytes().readPosition();
            final ValueIn read = inWire.read(() -> HANDLER);

            final Object o;

            if (read.isTyped())
                o = read.typedMarshallable();
            else {
                inWire.bytes().readPosition(readPosition);
                o = toSessionDetails(inWire);
            }

            handler = handlerFunction.apply(o);

            if (handler instanceof NetworkContextManager)
                ((NetworkContextManager) handler).nc(nc);

            handlerManager.tcpHandler(handler);

        } catch (Exception e) {
            LOG.error("wirein=" + Wires.fromSizePrefixedBlobs(in), e);
        }
    }

    @NotNull
    public SessionDetails toSessionDetails(Wire inWire) {
        VanillaSessionDetails sd = new VanillaSessionDetails();
        sd.readMarshallable(inWire);
        return sd;
    }

    @Override
    public void close() {
        this.nc.closeTask().close();
    }

}
