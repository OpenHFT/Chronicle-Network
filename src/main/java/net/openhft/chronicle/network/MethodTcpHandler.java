/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
    // Todo: investigate why outSetter is never used.
    private final Consumer<O> outSetter;
    private MethodReader reader;
    // Todo: investigate why output is never used.
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
    protected Wire initialiseOutWire(Bytes<?> out, @NotNull WireType wireType) {
        Wire wire = super.initialiseOutWire(out, wireType);
        output = wire.methodWriter(outClass);
        return wire;

    }

    @Override
    protected Wire initialiseInWire(@NotNull WireType wireType, Bytes<?> in) {
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
        // Do nothing
    }
}
