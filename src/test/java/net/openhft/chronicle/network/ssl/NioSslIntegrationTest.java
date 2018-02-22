package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.io.Closeable;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class NioSslIntegrationTest {
    private static final boolean SEND_DATA_BEFORE_SSL_HANDSHAKE = Boolean.getBoolean("ssl.test.payload");

    @Test
    public void shouldEncryptAndDecryptTraffic() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress("0.0.0.0", 13337));
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.configureBlocking(true);

        final SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", serverChannel.socket().getLocalPort()));

        try {

            final Client client = new Client(channel);

            final StateMachineProcessor clientProcessor = new StateMachineProcessor(channel, false,
                    SSLContextLoader.getInitialisedContext(), client);

            final SocketChannel serverConnection = serverChannel.accept();
            serverConnection.configureBlocking(false);

            final Server server = new Server(serverConnection);
            final StateMachineProcessor serverProcessor = new StateMachineProcessor(serverConnection, true,
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
        } finally {
            Closeable.closeQuietly(channel, serverChannel);
        }

        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
    }

    private void testDataConnection(final SocketChannel channel, final SocketChannel serverConnection) throws IOException {
        final ByteBuffer message = ByteBuffer.wrap("test message".getBytes(StandardCharsets.US_ASCII));

        while (message.hasRemaining()) {
            channel.write(message);
        }

        message.clear();
        while (message.hasRemaining()) {
            serverConnection.read(message);
        }

        message.flip();
        assertThat(new String(message.array()), is("test message"));
    }

    private static final class Client extends AbstractSocketBufferHandler {

        private final CountDownLatch latch = new CountDownLatch(1);
        private int counter = 0;
        private int responseCount = 0;

        Client(final SocketChannel socketChannel) {
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

        Server(final SocketChannel channel) {
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
            }
        }
    }
}