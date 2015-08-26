package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.network.connection.SocketAddressSupplier.uri;

/**
 * Created by rob on 26/08/2015.
 */
public class SimpleServerAndClientTest {

    @Test
    public void test() throws IOException {
        String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        EventGroup eg = new EventGroup(true);
        eg.start();
        String expectedMessage = "<my message>";
        createServer(desc, eg);

        try (TcpChannelHub tcpChannelHub = createClient(eg, desc)) {

            // create the message to send§
            final long tid = tcpChannelHub.nextUniqueTransaction(System.currentTimeMillis());
            final Wire wire = new TextWire(Bytes.elasticByteBuffer());

            wire.writeDocument(true, w -> w.write(() -> "tid").int64(tid));
            wire.writeDocument(false, w -> w.write(() -> "payload").text(expectedMessage));

            // write the data to the socket
            tcpChannelHub.lock(() -> tcpChannelHub.writeSocket(wire));

            // read the reply from the socket ( timeout after 1 second )
            Wire reply = tcpChannelHub.proxyReply(SECONDS.toMillis(1), tid);

            // read the reply and check the result
            reply.readDocument(null, data -> {
                final String text = data.read(() -> "payloadResponse").text();
                Assert.assertEquals(expectedMessage, text);
            });

            eg.stop();
            TcpChannelHub.closeAllHubs();
            TCPRegistry.reset();
            tcpChannelHub.close();
        }
    }

    @NotNull
    private TcpChannelHub createClient(EventGroup eg, String desc) {
        return new TcpChannelHub(null, eg, WireType.TEXT, "", uri(desc), false);
    }

    private void createServer(String desc, EventGroup eg) throws IOException {
        AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                () -> new EchoRequestHandler(WireType.TEXT), VanillaSessionDetails::new, 0, 0);
        eg.addHandler(eah);
        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }

    static class EchoRequestHandler extends WireTcpHandler {


        public EchoRequestHandler(@NotNull Function<Bytes, Wire> bytesToWire) {
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
                long tid = inWire.read(() -> "tid").int64();
                outWire.writeDocument(true, meta -> meta.write(() -> "tid").int64(tid));
            }, d -> {
                final String payload = inWire.read(() -> "payload").text();
                outWire.writeDocument(false, data -> data.write(() -> "payloadResponse").text(payload));
            });
        }
    }
}
