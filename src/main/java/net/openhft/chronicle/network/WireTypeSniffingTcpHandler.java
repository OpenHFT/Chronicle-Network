package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
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

    @Override
    public void close() {
        Closeable.closeQuietly(this.nc.closeTask());
    }

}
