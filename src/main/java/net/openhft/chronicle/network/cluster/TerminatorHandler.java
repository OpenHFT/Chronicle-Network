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

    public static WriteMarshallable terminationHandler(int localIdentifier, int remoteIdentifier, final long cid) {
        return w -> w.writeDocument(true,
                d -> d.writeEventName(CoreFields.csp).text("/")
                        .writeEventName(CoreFields.cid).int64(cid)
                        .writeEventName(CoreFields.handler).typedMarshallable(new TerminatorHandler())
                        .writeComment("localIdentifier=" + localIdentifier +
                                ",remoteIdentifier=" + remoteIdentifier));
    }

    @Override
    public void onInitialize(@NotNull WireOut outWire) {
        if (isClosed.getAndSet(true))
            return;
        nc().terminationEventHandler().onTerminate(nc());
        Closeable.closeQuietly(closable());
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
    }

    @Override
    public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {

    }

    @Override
    public void close() {

    }
}
