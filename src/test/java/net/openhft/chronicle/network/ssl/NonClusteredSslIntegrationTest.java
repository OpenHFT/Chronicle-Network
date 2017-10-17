package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Ignore
public final class NonClusteredSslIntegrationTest {
    private final EventGroup client = new EventGroup(true, Pauser.millis(1), false, "client");
    private final EventGroup server = new EventGroup(true, Pauser.millis(1), false, "server");
    private final CountingTcpHandler clientAcceptor = new CountingTcpHandler("client-acceptor");
    private final CountingTcpHandler serverAcceptor = new CountingTcpHandler("server-acceptor");
    private final CountingTcpHandler clientInitiator = new CountingTcpHandler("client-initiator");
    private final CountingTcpHandler serverInitiator = new CountingTcpHandler("server-initiator");

    @Before
    public void setUp() throws Exception {
        TCPRegistry.createServerSocketChannelFor("client", "server");
        client.start();
        server.start();

        client.addHandler(new AcceptorEventHandler("client", nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(clientAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

        server.addHandler(new AcceptorEventHandler("server", nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(serverAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

        new RemoteConnector(nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(clientInitiator));
            return eventHandler;
        }).connect("server", client, new StubNetworkContext(), 1000L);

        new RemoteConnector(nc -> {
            final TcpEventHandler eventHandler = new TcpEventHandler(nc);
            eventHandler.tcpHandler(new SslDelegatingTcpHandler<>(serverInitiator));
            return eventHandler;
        }).connect("client", server, new StubNetworkContext(), 1000L);
    }

    @Test
    public void shouldCommunicate() throws Exception {
        waitForLatch(clientAcceptor);
        waitForLatch(serverAcceptor);
        waitForLatch(clientInitiator);
        waitForLatch(serverInitiator);

        while (clientAcceptor.operationCount < 10 && serverAcceptor.operationCount < 10) {
            Thread.sleep(100);
        }

        assertTrue(clientAcceptor.operationCount > 9);
        assertTrue(serverAcceptor.operationCount > 9);
    }

    @After
    public void tearDown() throws Exception {
        client.stop();
        server.stop();
    }

    private static void waitForLatch(final CountingTcpHandler handler) throws InterruptedException {
        assertTrue(handler.label, handler.latch.await(5, TimeUnit.SECONDS));
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
            if (nc.isAcceptor() && in.readRemaining() != 0) {
                final long received = in.readLong();
                final int len = in.readInt();
                final byte[] tmp = new byte[len];
                in.read(tmp);
                System.out.printf("%s/0x%s received %d/%s%n", getClass().getSimpleName(),
                        Integer.toHexString(System.identityHashCode(this)), received,
                        new String(tmp));
                operationCount++;
            } else if (!nc.isAcceptor()) {
                if (System.currentTimeMillis() > lastSent + 100L) {
                    out.writeLong((counter++));
                    final String payload = "ping-" + (counter - 1);
                    out.writeInt(payload.length());
                    out.write(payload.getBytes(StandardCharsets.US_ASCII));
                    operationCount++;
                    lastSent = System.currentTimeMillis();
                }
            }

            latch.countDown();
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
