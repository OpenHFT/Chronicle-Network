package net.openhft.performance.tests.network;

import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

/**
 * This code is used to read the tid and payload from a wire message, and send the same tid and
 * message back to the client
 */
public class WireEchoRequestHandler extends WireTcpHandler {


    public WireEchoRequestHandler(NetworkContext networkContext) {
        super(networkContext);
    }

    /**
     * simply reads the csp,tid and payload and sends back the tid and payload
     *
     * @param inWire  the wire from the client
     * @param outWire the wire to be sent back to the server
     */
    @Override
    protected void process(@NotNull WireIn inWire,
                           @NotNull WireOut outWire) {

        System.out.println(Wires.fromSizePrefixedBlobs(inWire.bytes()));

        inWire.readDocument(m -> {
            outWire.writeDocument(true, meta -> meta.write(() -> "tid")
                    .int64(inWire.read(() -> "tid").int64()));
        }, d -> {
            outWire.writeDocument(false, data -> data.write(() -> "payloadResponse")
                    .text(inWire.read(() -> "payload").text()));
        });
    }
}
