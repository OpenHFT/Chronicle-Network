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
package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class HeaderTcpHandler<T extends NetworkContext<T>> extends SimpleCloseable implements TcpHandler<T> {

    public static final String HANDLER = "handler";
    private static final Logger LOG = LoggerFactory.getLogger(HeaderTcpHandler.class);
    @NotNull
    private final TcpEventHandler<T> handlerManager;
    @NotNull
    private final Function<Object, TcpHandler<T>> handlerFunction;

    public HeaderTcpHandler(@NotNull final TcpEventHandler<T> handlerManager,
                            @NotNull final Function<Object, TcpHandler<T>> handlerFunction) {
        this.handlerManager = handlerManager;
        this.handlerFunction = handlerFunction;
    }

    @Override
    public void process(@NotNull final Bytes in,
                        @NotNull final Bytes out,
                        @NotNull final T nc) {
        throwExceptionIfClosed();

        WireType wireType = nc.wireType() == null ? WireType.BINARY :  nc.wireType();
      // assert wireType != null;

        // the type of the header
        final Wire inWire = wireType.apply(in);
        final long start = in.readPosition();

        Object o = null;
        try (final DocumentContext dc = inWire.readingDocument()) {

            if (!dc.isPresent())
                return;

            if (YamlLogging.showServerReads())
                LOG.info("nc.isAcceptor=" + nc.isAcceptor() +
                        ", read:\n" + Wires.fromSizePrefixedBlobs(in, start, in.readLimit() - start));

            final long readPosition = inWire.bytes().readPosition();
            @NotNull final ValueIn read = inWire.read(() -> HANDLER);

            if (dc.isMetaData() && read.isTyped())
                o = read.typedMarshallable();
            else {
                inWire.bytes().readPosition(readPosition);
                o = toSessionDetails(inWire);
            }

            final TcpHandler handler = handlerFunction.apply(o);

            if (handler instanceof NetworkContextManager)
                ((NetworkContextManager<T>) handler).nc(nc);

            handlerManager.tcpHandler(handler);

        } catch (Exception e) {
            if (isClosed())
                return;
            Jvm.pause(50);
            if (isClosed())
                return;
            if (o instanceof SubHandler) {
                Jvm.warn().on(getClass(), "SubHandler " + o + " sent before UberHandler, closing.");
                close();

            } else {
                Jvm.warn().on(getClass(), "wirein=" + Wires.fromSizePrefixedBlobs(inWire), e);
            }
        }
    }

    @NotNull
    private SessionDetailsProvider toSessionDetails(@NotNull final Wire inWire) {
        @NotNull final VanillaSessionDetails sd = new VanillaSessionDetails();
        sd.readMarshallable(inWire);
        return sd;
    }
}