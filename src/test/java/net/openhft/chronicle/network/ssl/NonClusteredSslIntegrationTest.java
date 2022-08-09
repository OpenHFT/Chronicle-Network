/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLContext;
import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * IMPORTANT - each event handler MUST run in its own thread, as the handshake is a
 * blocking operation. Spinning the work out to a separate thread does not work,
 * as subsequent invocations of the TcpEventHandler may consume socket data during the
 * handshake.
 * <p>
 * <code>
 * public ServerThreadingStrategy serverThreadingStrategy() {
 *     return ServerThreadingStrategy.CONCURRENT;
 * }
 * </code>
 */
public final class NonClusteredSslIntegrationTest extends NetworkTestCommon {

    private static final boolean DEBUG = Jvm.getBoolean("NonClusteredSslIntegrationTest.debug");
    private final EventGroup client = EventGroup.builder().withPauser(Pauser.millis(1)).withName("client").build();
    private final EventGroup server = EventGroup.builder().withPauser(Pauser.millis(1)).withName("server").build();
    private final CountingTcpHandler clientAcceptor = new CountingTcpHandler("client-acceptor");
    private final CountingTcpHandler serverAcceptor = new CountingTcpHandler("server-acceptor");
    private final CountingTcpHandler clientInitiator = new CountingTcpHandler("client-initiator");
    private final CountingTcpHandler serverInitiator = new CountingTcpHandler("server-initiator");
    private Mode mode;

    public static List<Object[]> params() {
        final List<Object[]> params = new ArrayList<>();
        stream(Mode.values()).forEach(m -> params.add(new Object[]{m}));

        return params;
    }

    private static void waitForLatch(final CountingTcpHandler handler) throws InterruptedException {
        assertTrue(handler.latch.await(25, TimeUnit.SECONDS), "Handler for " + handler.label + " did not startup within timeout at " + Instant.now());
    }

    @BeforeEach
    void setUp() throws Exception {
        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("client", "server");

        client.addHandler(new AcceptorEventHandler<>("client", nc -> {
            final TcpEventHandler<StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
            eventHandler.tcpHandler(getTcpHandler(clientAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

        server.addHandler(new AcceptorEventHandler<>("server", nc -> {
            final TcpEventHandler<StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
            eventHandler.tcpHandler(getTcpHandler(serverAcceptor));
            return eventHandler;
        }, StubNetworkContext::new));

    }

    @ParameterizedTest
    @MethodSource("params")
    @Timeout(5)
    void shouldCommunicate(final Mode mode) throws Exception {
        assumeFalse(OS.isWindows() && mode == Mode.BI_DIRECTIONAL,
                "BI_DIRECTIONAL mode sometimes hangs during handshake on Windows");
        // Socket reconnector not provided.
        ignoreException("socketReconnector == null");

        this.mode = mode;
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

    @AfterEach
    void cleanUp() {
        client.stop();
        client.close();
        server.stop();
        server.close();
        TCPRegistry.reset();
        TCPRegistry.assertAllServersStopped();
    }

    private void doConnect() {
        if (mode == Mode.CLIENT_TO_SERVER || mode == Mode.BI_DIRECTIONAL) {
            new RemoteConnector<StubNetworkContext>(nc -> {
                final TcpEventHandler<StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
                eventHandler.tcpHandler(getTcpHandler(clientInitiator));
                return eventHandler;
            }).connect("server", client, new StubNetworkContext(), 1000L);
        }

        if (mode == Mode.SERVER_TO_CLIENT || mode == Mode.BI_DIRECTIONAL) {
            new RemoteConnector<StubNetworkContext>(nc -> {
                final TcpEventHandler<StubNetworkContext> eventHandler = new TcpEventHandler<>(nc);
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

    private static final class CountingTcpHandler
            extends SimpleCloseable
            implements TcpHandler<StubNetworkContext> {
        private final String label;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile long operationCount = 0;
        private long counter = 0;
        private long lastSent = 0;

        CountingTcpHandler(final String label) {
            this.label = label;
        }

        @Override
        public void process(@NotNull final Bytes<?> in, @NotNull final Bytes<?> out, final StubNetworkContext nc) {
            latch.countDown();
            try {
                if (nc.isAcceptor() && in.readRemaining() != 0) {
                    final int magic = in.readInt();
                    if (magic != 0xFEDCBA98)
                        throw new IllegalStateException("Invalid magic number " + Integer.toHexString(magic));
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
                    final String payload = "ping-" + (counter - 1);
                    if (System.currentTimeMillis() > lastSent + 100L) {
                        out.writeInt(0xFEDCBA98);
                        out.writeLong((counter++));
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

        @Override
        protected void performClose() {
        }
    }

    static final class StubNetworkContext
            extends VanillaNetworkContext<StubNetworkContext>
            implements SslNetworkContext<StubNetworkContext> {
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
        public StubNetworkContext socketChannel(final ChronicleSocketChannel socketChannel) {
            return super.socketChannel(socketChannel);
        }

        @Override
        public ServerThreadingStrategy serverThreadingStrategy() {
            return ServerThreadingStrategy.CONCURRENT;
        }

        @Override
        public NetworkStatsListener<StubNetworkContext> networkStatsListener() {
            return new NetworkStatsListener<StubNetworkContext>() {
                @Override
                public void networkContext(final StubNetworkContext networkContext) {

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

                @Override
                public boolean isClosed() {
                    return false;
                }
            };
        }
    }
}