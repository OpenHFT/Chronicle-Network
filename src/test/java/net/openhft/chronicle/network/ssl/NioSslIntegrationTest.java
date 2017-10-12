package net.openhft.chronicle.network.ssl;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public final class NioSslIntegrationTest {
    private static final File KEYSTORE_FILE = new File("src/test/resources", "test.jks");

    @Test
    public void shouldEncryptAndDecryptTraffic() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(null);
        serverChannel.configureBlocking(true);

        final SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", serverChannel.socket().getLocalPort()));

        final Client client = new Client();

        final StateMachineProcessor clientProcessor = new StateMachineProcessor(channel, false,
                client::onReceivedMessage, client::onAddApplicationData, getInitialisedContext());
        threadPool.submit(clientProcessor);

        final SocketChannel serverConnection = serverChannel.accept();
        serverConnection.configureBlocking(false);

        final Server server = new Server();
        final StateMachineProcessor serverProcessor = new StateMachineProcessor(serverConnection, true,
                server::onReceivedMessage, server::onAddApplicationData, getInitialisedContext());
        threadPool.submit(serverProcessor);

        client.waitForResponse(10, TimeUnit.SECONDS);

        serverProcessor.stop();
        clientProcessor.stop();
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static final class Client {
        private final CountDownLatch latch = new CountDownLatch(1);
        private int counter = 0;

        private void onAddApplicationData(final ByteBuffer outboundApplicationData) {
            outboundApplicationData.clear();
            outboundApplicationData.put(("hello " + (counter++)).getBytes(StandardCharsets.US_ASCII));
        }

        private void onReceivedMessage(final ByteBuffer buffer) {
            final byte[] tmp = new byte[buffer.remaining()];
            buffer.get(tmp);
            final String response = new String(tmp, StandardCharsets.UTF_8);

            if (response.equals("echo: hello 0")) {
                latch.countDown();
            }
        }

        void waitForResponse(final long duration, final TimeUnit timeUnit) throws InterruptedException {
            assertTrue(latch.await(duration, timeUnit));
        }

    }

    private static final class Server {
        private final ByteBuffer lastReceivedMessage = ByteBuffer.allocateDirect(64);

        private void onAddApplicationData(final ByteBuffer applicationData) {
            if (lastReceivedMessage.remaining() != lastReceivedMessage.capacity() && lastReceivedMessage.hasRemaining()) {
                applicationData.put("echo: ".getBytes(StandardCharsets.US_ASCII));
                applicationData.put(lastReceivedMessage);
            }
        }

        private void onReceivedMessage(final ByteBuffer buffer) {
            lastReceivedMessage.clear();
            lastReceivedMessage.put(buffer);
            lastReceivedMessage.flip();
        }
    }


    @NotNull
    private static SSLContext getInitialisedContext() throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        final SSLContext context = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf =
                KeyManagerFactory.getInstance("SunX509");
        final KeyStore keyStore = KeyStore.getInstance("JKS");
        final char[] password = "password".toCharArray();
        keyStore.load(new FileInputStream(KEYSTORE_FILE), password);
        kmf.init(keyStore, password);

        final KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new FileInputStream(KEYSTORE_FILE), password);

        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return context;
    }
}
