package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * used to send/receive a termination event, or receipt of the termination event the connection is
 * closed
 *
 * @author Rob Austin.
 */
public class TerminatorHandler extends AbstractSubHandler<NetworkContext> implements
        Demarshallable, WriteMarshallable {

    private final AtomicBoolean isClosed = new AtomicBoolean();

    @UsedViaReflection
    private TerminatorHandler(WireIn w) {

    }

    private TerminatorHandler() {

    }

    @Override
    public void onInitialize(@NotNull WireOut outWire) {
        if (isClosed.getAndSet(true))
            return;
        nc().terminationEventHandler().onTerminate();
        Closeable.closeQuietly(closable());
    }

    public static WriteMarshallable terminationHandler() {
        return w -> w.writeDocument(true,
                d -> d.writeEventName(CoreFields.csp).text("/")
                        .writeEventName(CoreFields.cid).int64(TerminatorHandler.class.hashCode())
                        .writeEventName(CoreFields.handler).typedMarshallable(new TerminatorHandler()));
    }


    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
    }

    @Override
    public void processData(@NotNull WireIn inWire, @NotNull WireOut outWire) {

    }

    @Override
    public void close() {

    }
}
