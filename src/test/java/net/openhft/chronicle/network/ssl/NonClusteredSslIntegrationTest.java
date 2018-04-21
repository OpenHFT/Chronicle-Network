package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.stream;
import static org.junit.Assert.assertTrue;

/**
 * IMPORTANT - each event handler MUST run in its own thread, as the handshake is a
 * blocking operation. Spinning the work out to a separate thread does not work,
 * as subsequent invocations of the TcpEventHandler may consume socket data during the
 * handshake.
 * <p>
 * public ServerThreadingStrategy serverThreadingStrategy() {
 * return ServerThreadingStrategy.MULTI_THREADED_BUSY_WAITING;
 * }
 */
@RunWith(Parameterized.class)
public final class NonClusteredSslIntegrationTest {

    private static final boolean DEBUG = Boolean.getBoolean("NonClusteredSslIntegrationTest.debug");
    private final EventGroup client = new EventGroup(true, Pauser.millis(1), false, "client");
    private final EventGroup server = new EventGroup(true, Pauser.millis(1), false, "server");
    private final CountingTcpHandler clientAcceptor = new CountingTcpHandler("client-acceptor");
    private final CountingTcpHandler serverAcceptor = new CountingTcpHandler("server-acceptor");
    private final CountingTcpHandler clientInitiator = new CountingTcpHandler("client-initiator");
    private final CountingTcpHandler serverInitiator = new CountingTcpHandler("server-initiator");
    private final Mode mode;

    public NonClusteredSslIntegrationTest(final String name, final Mode mode) {
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> params() {
        final List<Object[]> params = new ArrayList<>();
        stream(Mode.values()).forEach(m -> params.add(new Object[]{m.name(), m}));

        return params;
    }

    private static void waitForLatch(final CountingTcpHandler handler) throws InterruptedException {
        assertTrue("Handler for " + handler.label + " did not startup within timeout at " + Instant.now(),
                handler.latch.await(25, TimeUnit.SECONDS));
    }

    @Before
    public void setUp() throws Exception {
        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("client", "server");

        client.addHandler(new AcceptorEventHandler("client", nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(getTcpHandler(clientAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

        server.addHandler(new AcceptorEventHandler("server", nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(getTcpHandler(serverAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

    }

    @Test(timeout = 40_000L)
    public void shouldCommunicate() throws Exception {
        Assume.assumeFalse("BI_DIRECTIONAL mode sometimes hangs during handshake", mode == Mode.BI_DIRECTIONAL);
        client.start();
        server.start();
        doConnect();

        switch (mode) {
            case CLIENT_TO_SERVER:
                assertThatClientConnectsToServer();
                break;
            case SERVER_TO_CLIENT:
                assertThatServerConnectsToClient();
                break;
            default:
                assertThatClientConnectsToServer();
                assertThatServerConnectsToClient();
                break;
        }

    }

    @After
    public void tearDown() throws Exception {
        client.close();
        client.stop();
        server.close();
        server.stop();
        TCPRegistry.reset();
        TCPRegistry.assertAllServersStopped();
    }

    private void doConnect() {
        if (mode == Mode.CLIENT_TO_SERVER || mode == Mode.BI_DIRECTIONAL) {
            new RemoteConnector(nc -> {
                final TcpEventHandler eventHandler = new TcpEventHandler(nc);
                eventHandler.tcpHandler(getTcpHandler(clientInitiator));
                return eventHandler;
            }).connect("server", client, new StubNetworkContext(), 1000L);
        }

        if (mode == Mode.SERVER_TO_CLIENT || mode == Mode.BI_DIRECTIONAL) {
            new RemoteConnector(nc -> {
                final TcpEventHandler eventHandler = new TcpEventHandler(nc);
                eventHandler.tcpHandler(getTcpHandler(serverInitiator));
                return eventHandler;
            }).connect("client", server, new StubNetworkContext(), 1000L);
        }
    }

    @NotNull
    private TcpHandler<StubNetworkContext> getTcpHandler(final CountingTcpHandler delegate) {
        return new SslDelegatingTcpHandler<>(delegate);
    }

    private void assertThatServerConnectsToClient() throws InterruptedException {
        waitForLatch(clientAcceptor);
        waitForLatch(serverInitiator);

        while (serverInitiator.operationCount < 10 || clientAcceptor.operationCount < 10) {
            Thread.sleep(100);
        }

        assertTrue(serverInitiator.operationCount > 9);
        assertTrue(clientAcceptor.operationCount > 9);
    }

    private void assertThatClientConnectsToServer() throws InterruptedException {
        waitForLatch(serverAcceptor);
        waitForLatch(clientInitiator);

        while (clientInitiator.operationCount < 10 || serverAcceptor.operationCount < 10) {
            Thread.sleep(100);
        }

        assertTrue(clientInitiator.operationCount > 9);
        assertTrue(serverAcceptor.operationCount > 9);
    }

    private enum Mode {
        CLIENT_TO_SERVER,
        SERVER_TO_CLIENT,
        BI_DIRECTIONAL
    }

    private static final class CountingTcpHandler implements TcpHandler<StubNetworkContext> {
        private final String label;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile long operationCount = 0;
        private long counter = 0;
        private long lastSent = 0;

        CountingTcpHandler(final String label) {
            this.label = label;
        }

        @Override
        public void process(@NotNull final Bytes in, @NotNull final Bytes out, final StubNetworkContext nc) {
            latch.countDown();
            try {
                if (nc.isAcceptor() && in.readRemaining() != 0) {
                    final long received = in.readLong();
                    final int len = in.readInt();
                    final byte[] tmp = new byte[len];
                    in.read(tmp);
                    if (DEBUG) {
                        if (len > 10) {
                            System.out.printf("%s received payload of length %d%n", label, len);
                            System.out.println(in);
                        } else {
                            System.out.printf("%s received [%d] %d/%s%n", label, tmp.length, received, new String(tmp, StandardCharsets.US_ASCII));
                        }
                    }
                    operationCount++;
                } else if (!nc.isAcceptor()) {
                    if (System.currentTimeMillis() > lastSent + 100L) {
                        out.writeLong((counter++));
                        final String payload = "ping-" + (counter - 1);
                        out.writeInt(payload.length());
                        out.write(payload.getBytes(StandardCharsets.US_ASCII));
                        if (DEBUG) {
                            System.out.printf("%s sent [%d] %d/%s%n", label, payload.length(), counter - 1, payload);
                        }
                        operationCount++;
                        lastSent = System.currentTimeMillis();
                    }
                }

            } catch (RuntimeException e) {
                System.err.printf("Exception in %s: %s/%s%n", label, e.getClass().getSimpleName(), e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static final class StubNetworkContext extends VanillaNetworkContext
            implements SslNetworkContext {
        @Override
        public SSLContext sslContext() {
            try {
                return SSLContextLoader.getInitialisedContext();
            } catch (NoSuchAlgorithmException | KeyStoreException | IOException |
                    CertificateException | UnrecoverableKeyException | KeyManagementException e) {
                throw new RuntimeException("Failed to initialise ssl context", e);
            }
        }

        @NotNull
        @Override
        public VanillaNetworkContext socketChannel(final SocketChannel socketChannel) {
            return super.socketChannel(socketChannel);
        }

        @Override
        public ServerThreadingStrategy serverThreadingStrategy() {
            return ServerThreadingStrategy.MULTI_THREADED_BUSY_WAITING;
        }

        @Override
        public NetworkStatsListener<? extends NetworkContext> networkStatsListener() {
            return new NetworkStatsListener<NetworkContext>() {
                @Override
                public void networkContext(final NetworkContext networkContext) {

                }

                @Override
                public void onNetworkStats(final long writeBps, final long readBps, final long socketPollCountPerSecond) {

                }

                @Override
                public void onHostPort(final String hostName, final int port) {

                }

                @Override
                public void onRoundTripLatency(final long nanosecondLatency) {

                }

                @Override
                public void close() {

                }
            };
        }
    }
}