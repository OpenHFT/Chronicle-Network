package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.RejectedExecutionException;

import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;

final class PingTcpHandler extends AbstractSubHandler<SslTestClusteredNetworkContext> implements Marshallable {

    PingTcpHandler() {
        System.out.printf("0x%s constructed%n", toHexString(identityHashCode(this)));
    }

    @NotNull
    static WriteMarshallable newPingHandler(final String csp, final long cid) {
        @NotNull final PingTcpHandler handler = new PingTcpHandler();

        return w -> w.writeDocument(true, d -> d.writeEventName(CoreFields.csp).text(csp)
                .writeEventName(CoreFields.cid).int64(cid)
                .writeEventName(CoreFields.handler).typedMarshallable(handler));
    }

    @Override
    public void readMarshallable(@NotNull final WireIn wire) throws IORuntimeException {
    }

    @Override
    public void onRead(@NotNull final WireIn inWire, @NotNull final WireOut outWire) {
        final StringBuilder eventName = Wires.acquireStringBuilder();
        @NotNull final ValueIn valueIn = inWire.readEventName(eventName);
        if ("ping".contentEquals(eventName)) {
            final long id = valueIn.int64();
            System.out.printf("%d received ping %d from %d%n", localIdentifier(), id, remoteIdentifier());

            nc().wireOutPublisher().put(null, wireOut -> {
                wireOut.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
                wireOut.writeDocument(false,
                        d -> d.writeEventName("pong").int64(id));
            });
        } else if ("pong".contentEquals(eventName)) {
            final long id = valueIn.int64();
            System.out.printf("%d received pong %d from %d%n", localIdentifier(), id, remoteIdentifier());
        }
    }

    @Override
    public void onInitialize(final WireOut outWire) throws RejectedExecutionException {
        System.out.printf("0x%s initialised%n", toHexString(identityHashCode(this)));
        if (nc().isAcceptor()) {
            @NotNull WriteMarshallable writeMarshallable = newPingHandler(csp(), cid());
            publish(writeMarshallable);

            nc().eventLoop().addHandler(true, new PingSender(this::nc, this::localIdentifier, this::remoteIdentifier, cid()));
        }
    }
}
