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
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static net.openhft.chronicle.wire.WireType.*;

/**
 * sets the wire-type in the network context by inspecting the byte message
 *
 * @author Rob Austin.
 */
public class WireTypeSniffingTcpHandler<T extends NetworkContext> implements TcpHandler<T> {

    @NotNull
    private final TcpEventHandler handlerManager;

    @NotNull
    private final Function<T, TcpHandler> delegateHandlerFactory;

    public WireTypeSniffingTcpHandler(@NotNull final TcpEventHandler handlerManager,
                                      @NotNull final Function<T, TcpHandler> delegateHandlerFactory) {
        this.handlerManager = handlerManager;
        this.delegateHandlerFactory = delegateHandlerFactory;
    }

    @Override
    public void process(final @NotNull Bytes in,
                        final @NotNull Bytes out,
                        T nc) {

        final WireOutPublisher publisher = nc.wireOutPublisher();

        if (publisher != null)
            publisher.applyAction(out);

        // read the wire type of the messages from the header - the header its self must be
        // of type TEXT or BINARY
        if (in.readRemaining() < 5)
            return;

        final int required = Wires.lengthOf(in.readInt(in.readPosition()));

        assert required < 10 << 20;

        if (in.readRemaining() < required + 4)
            return;

        final byte b = in.readByte(4);

        @NotNull final WireType wireType;
        if (b < 0)
            wireType = DELTA_BINARY.isAvailable() ? DELTA_BINARY : BINARY;
        else if (b > ' ')
            wireType = TEXT;
        else
            throw new IllegalStateException("Unable to identify the wire type from " + Integer.toHexString(b & 0xFF));

        // the type of the header
        nc.wireType(wireType);

        final TcpHandler handler = delegateHandlerFactory.apply(nc);

        if (handler instanceof NetworkContextManager)
            ((NetworkContextManager) handler).nc(nc);

        handlerManager.tcpHandler(handler);
    }
}
