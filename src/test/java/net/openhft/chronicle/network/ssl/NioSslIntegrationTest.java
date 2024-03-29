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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketFactory;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NioSslIntegrationTest extends NetworkTestCommon {
    private static final boolean SEND_DATA_BEFORE_SSL_HANDSHAKE = Jvm.getBoolean("ssl.test.payload");

    @BeforeEach
    void setUp() {
        // https://bugs.openjdk.java.net/browse/JDK-8211426
        if (Jvm.majorVersion() >= 11)
            System.setProperty("jdk.tls.server.protocols", "TLSv1.2");
    }

    @Override
    @BeforeEach
    protected void threadDump() {
        super.threadDump();
    }

    @AfterEach
    void teardown() {
        System.clearProperty("jdk.tls.server.protocols");
    }

    @Test
    void shouldEncryptAndDecryptTraffic() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2,
                new NamedThreadFactory("test"));

        final ChronicleServerSocketChannel serverChannel = ChronicleServerSocketFactory.open("localhost:13337");
        serverChannel.bind(new InetSocketAddress("0.0.0.0", 13337));
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.configureBlocking(true);

        final ChronicleSocketChannel channel = ChronicleSocketChannelFactory.wrap();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", serverChannel.socket().getLocalPort()));

        try {

            final Client client = new Client(channel);

            final StateMachineProcessor clientProcessor = new StateMachineProcessor(false,
                    SSLContextLoader.getInitialisedContext(), client);

            final ChronicleSocketChannel serverConnection = serverChannel.accept();
            serverConnection.configureBlocking(false);

            final Server server = new Server(serverConnection);
            final StateMachineProcessor serverProcessor = new StateMachineProcessor(true,
                    SSLContextLoader.getInitialisedContext(), server);

            while (!(channel.finishConnect() && serverConnection.finishConnect())) {
                Thread.yield();
            }

            if (SEND_DATA_BEFORE_SSL_HANDSHAKE) {
                testDataConnection(channel, serverConnection);
            }

            threadPool.submit(clientProcessor);
            threadPool.submit(serverProcessor);

            client.waitForResponse(10, TimeUnit.SECONDS);
            serverProcessor.stop();
            clientProcessor.stop();
            serverConnection.close();
        } finally {
            Closeable.closeQuietly(channel, serverChannel);
        }

        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
    }

    private void testDataConnection(final ChronicleSocketChannel channel, final ChronicleSocketChannel serverConnection) throws IOException {
        final ByteBuffer message = ByteBuffer.wrap("test message".getBytes(StandardCharsets.US_ASCII));

        while (message.hasRemaining()) {
            channel.write(message);
        }

        message.clear();
        while (message.hasRemaining()) {
            serverConnection.read(message);
        }

        message.flip();
        assertEquals("test message", new String(message.array()));
    }

    private static final class Client extends AbstractSocketBufferHandler {

        private final CountDownLatch latch = new CountDownLatch(1);
        private int counter = 0;
        private int responseCount = 0;

        Client(final ChronicleSocketChannel socketChannel) {
            super(socketChannel);
        }

        void waitForResponse(final long duration, final TimeUnit timeUnit) throws InterruptedException {
            assertTrue(latch.await(duration, timeUnit));
        }

        @Override
        public void handleDecryptedData(final ByteBuffer input, final ByteBuffer output) {
            if (input.hasRemaining() && input.remaining() != input.capacity()) {
                final byte[] tmp = new byte[input.remaining()];
                input.get(tmp);
                final String response = new String(tmp, StandardCharsets.UTF_8);

                if (responseCount++ == 5) {
                    latch.countDown();
                }
            }
            output.put(("hello " + (counter++)).getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static final class Server extends AbstractSocketBufferHandler {
        private final ByteBuffer lastReceivedMessage = ByteBuffer.allocateDirect(64);

        Server(final ChronicleSocketChannel channel) {
            super(channel);
        }

        @Override
        public void handleDecryptedData(final ByteBuffer input, final ByteBuffer output) {
            if (input.hasRemaining() && input.remaining() != input.capacity()) {
                lastReceivedMessage.clear();
                lastReceivedMessage.put(input);
                lastReceivedMessage.flip();
            }

            if (lastReceivedMessage.remaining() != lastReceivedMessage.capacity() && lastReceivedMessage.hasRemaining()) {
                output.put("echo: ".getBytes(StandardCharsets.US_ASCII));
                output.put(lastReceivedMessage);
                lastReceivedMessage.clear();
            }
        }
    }
}