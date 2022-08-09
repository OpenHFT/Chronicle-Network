package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * This test can be used to make sure that SSL pathway has acceptable performance and does not produce extra garbage.
 * <p>
 * Expected performance:
 * <tt>Read total: 14890275 in 6664ms at rate 2234435b/s</tt>
 */
final class TransparentSslPerformanceTest extends NetworkTestCommon {
    private static boolean VERBOSE = false;
    private static final int RESPONSE_COUNT = VERBOSE ? 5_000 : 500_000;
    private static final byte[] HELLO = "hello ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ECHO = "echo: ".getBytes(StandardCharsets.US_ASCII);

    @BeforeEach
    void setUp() throws IOException {
        // https://bugs.openjdk.java.net/browse/JDK-8211426
        if (Jvm.majorVersion() >= 11)
            System.setProperty("jdk.tls.server.protocols", "TLSv1.2");

        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("client", "server");
    }

    @AfterEach
    void teardown() {
        System.clearProperty("jdk.tls.server.protocols");
    }

    @Test
    void shouldEncryptAndDecryptTraffic() throws Exception {
        assumeFalse(Jvm.isArm()); // Likely not suitable to display required performance

        ignoreException("socketReconnector == null");

        try (EventGroup client = EventGroup.builder().withPauser(Pauser.millis(1)).withName("client").build();
            EventGroup server = EventGroup.builder().withPauser(Pauser.millis(1)).withName("server").build()) {

            client.start();
            server.start();
            final Client clientHandler = new Client();

            server.addHandler(new AcceptorEventHandler<>("server", nc -> {
                final TcpEventHandler<NonClusteredSslIntegrationTest.StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
                eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(new Server()));
                return eventHandler;
            }, NonClusteredSslIntegrationTest.StubNetworkContext::new));

            long start = 0L;
            try (RemoteConnector<NonClusteredSslIntegrationTest.StubNetworkContext> stubNetworkContextRemoteConnector = new RemoteConnector<>(nc -> {
                final TcpEventHandler<NonClusteredSslIntegrationTest.StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
                eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(clientHandler));
                return eventHandler;
            })) {
                stubNetworkContextRemoteConnector.connect("server", client,
                        new NonClusteredSslIntegrationTest.StubNetworkContext(), 100L);
                start = System.currentTimeMillis();

                clientHandler.waitForResponse(30, TimeUnit.SECONDS);
            } finally {
                client.stop();
                server.stop();

                long end = System.currentTimeMillis();
                long readRate = (clientHandler.readTotal * 1000L) / (end - start);
                System.out.println("Read total " + clientHandler.readTotal + "b in " + (end - start) + "ms @ " +
                        readRate + "b/s");

                if (!VERBOSE) {
                    assertTrue(clientHandler.readTotal > 10_000_000L);
                    assertTrue(readRate > 1_000_000L);
                }
            }
        }
    }

    private static final class Client extends SimpleCloseable implements TcpHandler {

        private final CountDownLatch latch = new CountDownLatch(1);
        private int counter = 0;
        private int responseCount = 0;
        public int readTotal = 0;

        void waitForResponse(final long duration, final TimeUnit timeUnit) throws InterruptedException {
            assertTrue(latch.await(duration, timeUnit));
        }

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, NetworkContext nc) {
            if (in.readRemaining() > 0) {
                readTotal += in.readRemaining();
                if (VERBOSE) {
                    final byte[] tmp = new byte[(int) in.readRemaining()];
                    in.read(tmp);
                    final String response = new String(tmp, StandardCharsets.UTF_8);
                    System.err.println(response);
                } else {
                    in.readPosition(in.writePosition());
                }

                if (responseCount++ == RESPONSE_COUNT) {
                    latch.countDown();
                }
            }
            out.write(HELLO);
            int counter0 = counter++;
            do {
                out.writeByte((byte) ('0' + (counter0 % 10)));
                counter0 /= 10;
            } while (counter0 != 0);
        }
    }

    private static final class Server extends SimpleCloseable implements TcpHandler {
        private final ByteBuffer lastReceivedMessage = ByteBuffer.allocateDirect(64);

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, NetworkContext nc) {
            if (in.readRemaining() > 0) {
                lastReceivedMessage.clear();
                in.read(lastReceivedMessage);
                lastReceivedMessage.flip();
            }

            if (lastReceivedMessage.remaining() != lastReceivedMessage.capacity() && lastReceivedMessage.hasRemaining()) {
                out.write(ECHO);
                out.writeSome(lastReceivedMessage);
            }
        }
    }
}