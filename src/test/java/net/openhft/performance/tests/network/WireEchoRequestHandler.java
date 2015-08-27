package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * Created by rob on 26/08/2015.
 */
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
        inWire.readDocument(null, d -> outWire.writeDocument(false, data -> data
                .writeEventName(() -> "payloadResponse").text(inWire.readEventName(new StringBuilder()).text())));
    }
}
