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

package net.openhft.performance.tests.network;

import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * This code is used to read the tid and payload from a wire message, and send the same tid and
 * message back to the client
 */
public class WireEchoRequestHandler extends WireTcpHandler {

    public WireEchoRequestHandler(NetworkContext networkContext) {

    }

    /**
     * simply reads the csp,tid and payload and sends back the tid and payload
     *
     * @param in      the DocumentContext from the client
     * @param outWire the wire to be sent back to the server
     */
    @Override
    protected void onRead(@NotNull DocumentContext in,
                          @NotNull WireOut outWire) {

        if (in.isMetaData())
            outWire.writeDocument(true, meta -> meta.write(() -> "tid")
                    .int64(in.wire().read(() -> "tid").int64()));
        else
            outWire.writeDocument(false, data -> data.write(() -> "payloadResponse")
                    .text(in.wire().read(() -> "payload").text()));

    }

    @Override
    protected void onInitialize() {

    }
}
