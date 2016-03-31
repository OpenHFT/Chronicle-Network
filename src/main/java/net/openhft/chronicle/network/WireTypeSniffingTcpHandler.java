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

import static net.openhft.chronicle.wire.WireType.BINARY;
import static net.openhft.chronicle.wire.WireType.TEXT;

/**
 * sets the wire-type in the network context by inspecting the byte message
 *
 * @author Rob Austin.
 */
public class WireTypeSniffingTcpHandler<T extends NetworkContext> implements TcpHandler {

    private final TcpEventHandler handlerManager;

    private final T nc;
    private final Function<T, TcpHandler> delegateHandlerFactory;

    public WireTypeSniffingTcpHandler(@NotNull final TcpEventHandler handlerManager,
                                      @NotNull T nc,
                                      @NotNull Function<T, TcpHandler> delegateHandlerFactory) {
        this.handlerManager = handlerManager;
        this.nc = nc;
        this.delegateHandlerFactory = delegateHandlerFactory;
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out) {

        final WireOutPublisher publisher = nc.wireOutPublisher();

        if (publisher != null && out.writePosition() < TcpEventHandler.TCP_BUFFER)
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
        final WireType wireType = (b & 0x80) == 0 ? TEXT : BINARY;

        // the type of the header
        nc.wireType(wireType);

        final TcpHandler handler = delegateHandlerFactory.apply(nc);

        if (handler instanceof NetworkContextManager)
            ((NetworkContextManager) handler).nc(nc);

        handlerManager.tcpHandler(handler);
    }


}
