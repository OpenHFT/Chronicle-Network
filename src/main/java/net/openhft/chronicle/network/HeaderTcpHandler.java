package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class HeaderTcpHandler implements TcpHandler {

    private final TcpEventHandler handlerManager;
    private final Function<Marshallable, TcpHandler> handlerFunction;
    private final NetworkContext nc;

    public HeaderTcpHandler(@NotNull final TcpEventHandler handlerManager,
                            @NotNull final Function<Marshallable, TcpHandler> handlerFunction,
                            @NotNull final NetworkContext nc) {
        this.handlerManager = handlerManager;
        this.handlerFunction = handlerFunction;
        this.nc = nc;
    }

    @Override
    public void process(@NotNull Bytes in, @NotNull Bytes out) {

        // the type of the header
        final Wire inWire = nc.wireType().apply(in);

        try (final DocumentContext dc = inWire.readingDocument()) {

            if (!dc.isPresent())
                return;

            if (dc.isMetaData())
                throw new IllegalStateException("expecting a header of type data");

            final ValueIn valueIn = inWire.getValueIn();

            Marshallable marshallable;

            if (valueIn.isTyped())
                marshallable = valueIn.typedMarshallable();
            else {
                marshallable = new VanillaSessionDetails();
                valueIn.marshallable(marshallable);
            }

            final TcpHandler handler = handlerFunction.apply(marshallable);
            handlerManager.tcpHandler(handler);
        }


    }


}
