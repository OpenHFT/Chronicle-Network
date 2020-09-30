/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class WireEchoRequestHandler extends WireTcpHandler {

    public WireEchoRequestHandler(@NotNull Function<Bytes, Wire> bytesToWire) {
        super(bytesToWire);
    }

    /**
     * simply reads the csp,tid and payload and sends back the tid and payload
     *
     * @param inWire  the wire from the client
     * @param outWire the wire to be sent back to the server
     * @param sd      details about this session
     */
    @Override
    protected void process(@NotNull WireIn inWire,
                           @NotNull WireOut outWire,
                           @NotNull SessionDetailsProvider sd) {

        inWire.readDocument(m -> {
            long tid = inWire.read("tid").int64();
            outWire.writeDocument(true, meta -> meta.write("tid")
                    .int64(tid));

        }, d -> {
            outWire.writeDocument(false, data -> data.write("payloadResponse")
                    .text(inWire.read("payload").text()));
        });
    }
}
