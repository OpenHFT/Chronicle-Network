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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class HeaderTcpHandler<T extends NetworkContext> implements TcpHandler {

    public static final String HANDLER = "handler";
    private static final Logger LOG = LoggerFactory.getLogger(HeaderTcpHandler.class);
    @NotNull
    private final TcpEventHandler handlerManager;
    @NotNull
    private final Function<Object, TcpHandler> handlerFunction;
    @NotNull
    private final NetworkContext nc;

    public HeaderTcpHandler(@NotNull final TcpEventHandler handlerManager,
                            @NotNull final Function<Object, TcpHandler> handlerFunction,
                            @NotNull final T nc) {
        this.handlerManager = handlerManager;
        this.handlerFunction = handlerFunction;
        this.nc = nc;
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out, NetworkContext nc) {

        assert nc.wireType() != null;

        // the type of the header
        final Wire inWire = nc.wireType().apply(in);

        long start = in.readPosition();

        try (final DocumentContext dc = inWire.readingDocument()) {

            if (!dc.isPresent())
                return;

            if (YamlLogging.showServerReads())
                LOG.info("nc.isAcceptor=" + nc.isAcceptor() +
                        ", read:\n" + Wires.fromSizePrefixedBlobs(in, start, in.readLimit() - start));
            final TcpHandler handler;
            final long readPosition = inWire.bytes().readPosition();
            @NotNull final ValueIn read = inWire.read(() -> HANDLER);

            @Nullable final Object o;

            if (dc.isMetaData() && read.isTyped())
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
            Jvm.warn().on(getClass(), "wirein=" + Wires.fromSizePrefixedBlobs(inWire), e);
        }
    }

    @NotNull
    private SessionDetailsProvider toSessionDetails(@NotNull Wire inWire) {
        @NotNull VanillaSessionDetails sd = new VanillaSessionDetails();
        sd.readMarshallable(inWire);
        return sd;
    }
}
