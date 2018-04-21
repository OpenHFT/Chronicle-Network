package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class MethodTcpHandler<I, O, N extends NetworkContext<N>> extends WireTcpHandler<N> {

    private final Supplier<I> implSupplier;
    private final Class<O> outClass;
    private final Consumer<O> outSetter;
    private MethodReader reader;
    private O output;

    /**
     * This TcpHandler turns messages into method calls.
     *
     * @param implSupplier supplier for the object implementing the inbound messages
     * @param outClass     proxy to call for outbound messages
     * @param outSetter    setter to call when the output is initialised/changed
     */
    public MethodTcpHandler(Supplier<I> implSupplier, Class<O> outClass, Consumer<O> outSetter) {
        this.implSupplier = implSupplier;
        this.outClass = outClass;
        this.outSetter = outSetter;
    }

    @Override
    protected Wire initialiseOutWire(Bytes out, @NotNull WireType wireType) {
        Wire wire = super.initialiseOutWire(out, wireType);
        output = wire.methodWriter(outClass);
        return wire;

    }

    @Override
    protected Wire initialiseInWire(@NotNull WireType wireType, Bytes in) {
        Wire wire = super.initialiseInWire(wireType, in);
        reader = wire.methodReader(implSupplier.get());
        return wire;
    }

    @Override
    protected void onRead(@NotNull DocumentContext in, @NotNull WireOut out) {
        for (; ; ) {
            long pos = in.wire().bytes().readPosition();
            if (!reader.readOne())
                return;
            if (pos <= in.wire().bytes().readPosition()) {
                Jvm.warn().on(getClass(), "unable to parse data at the end of message " + in.wire().bytes().toDebugString());
                return;
            }
        }
    }

    @Override
    protected void onInitialize() {

    }
}
