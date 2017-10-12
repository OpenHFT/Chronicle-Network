package net.openhft.performance.tests.network;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public final class NioSslExample {

    private static final File KEYSTORE_FILE = new File("src/test/resources", "test.jks");

    public static void main(String[] args) throws IOException {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final Server server = new Server();
        threadPool.submit(server::run);
        final Client client = new Client("127.0.0.1", server.port());
        threadPool.submit(client::run);
    }

    private static final class Client {
        private final int serverPort;
        private final String hostname;

        private Client(final String host, final int serverPort) {
            this.serverPort = serverPort;
            hostname = host;
        }

        void run() {
            try {
                final SocketChannel channel = SocketChannel.open(new InetSocketAddress(hostname, serverPort));
                channel.configureBlocking(false);
                while (!channel.finishConnect()) {
                    Thread.yield();
                }
                final SSLContext context = getInitialisedContext();
                final SSLEngine engine = context.createSSLEngine();
                engine.setUseClientMode(true);
                new Handshaker().performHandshake(engine, channel);

                int counter = 0;

                final SslEngineStateMachine stateMachine =
                        new SslEngineStateMachine(engine, channel, this::onReceivedMessage);

                while (!Thread.currentThread().isInterrupted()) {
                    final ByteBuffer outboundApplicationData = stateMachine.getOutboundApplicationData();

                    outboundApplicationData.clear();
                    outboundApplicationData.put(("hello " + (counter++)).getBytes(StandardCharsets.US_ASCII));
                    while (stateMachine.action()) {
                        // process loop
                    }
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));
                }

            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        private void onReceivedMessage(final ByteBuffer buffer) {
            final byte[] tmp = new byte[buffer.remaining()];
            buffer.get(tmp);
            System.out.println(new String(tmp, StandardCharsets.UTF_8));
        }

    }

    private static final class Server {
        private final ServerSocketChannel serverChannel;
        private SslEngineStateMachine stateMachine;

        private Server() throws IOException {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(null);
            serverChannel.configureBlocking(true);
        }

        void run() {
            try {
                final SocketChannel channel = serverChannel.accept();
                channel.configureBlocking(false);

                final SSLContext context = getInitialisedContext();
                final SSLEngine engine = context.createSSLEngine();
                engine.setUseClientMode(false);
                engine.setNeedClientAuth(true);

                new Handshaker().performHandshake(engine, channel);

                stateMachine = new SslEngineStateMachine(engine, channel, this::onReceivedMessage);

                while (!Thread.currentThread().isInterrupted()) {
                    while (stateMachine.action()) {
                        // process loop
                    }
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));
                }

            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        private void onReceivedMessage(final ByteBuffer buffer) {
            final ByteBuffer applicationData = stateMachine.getOutboundApplicationData();
            applicationData.clear();
            applicationData.put("echo: ".getBytes(StandardCharsets.US_ASCII));
            applicationData.put(buffer);

        }

        int port() {
            return serverChannel.socket().getLocalPort();
        }
    }


    private static final class SslEngineStateMachine implements EventHandler {
        private final SSLEngine engine;
        private final SocketChannel channel;
        private final Consumer<ByteBuffer> decodedMessageReceiver;
        private ByteBuffer outboundApplicationData;
        private ByteBuffer outboundEncodedData;
        private ByteBuffer inboundEncodedData;
        private ByteBuffer inboundApplicationData;

        private SslEngineStateMachine(
                final SSLEngine engine, final SocketChannel channel,
                final Consumer<ByteBuffer> decodedMessageReceiver) {
            this.engine = engine;
            outboundApplicationData = ByteBuffer.allocateDirect(engine.getSession().getApplicationBufferSize());
            outboundEncodedData = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
            inboundApplicationData = ByteBuffer.allocateDirect(engine.getSession().getApplicationBufferSize());
            inboundEncodedData = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
            this.channel = channel;
            this.decodedMessageReceiver = decodedMessageReceiver;
        }

        ByteBuffer getOutboundApplicationData() {
            return outboundApplicationData;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            final int read;
            try {
                if (outboundApplicationData.position() != 0) {

                    outboundApplicationData.flip();
                    if (engine.wrap(outboundApplicationData, outboundEncodedData).
                            getStatus() == SSLEngineResult.Status.CLOSED) {
                        throw new InvalidEventHandlerException("Socket closed");
                    }
                    outboundApplicationData.compact();

                }
                if (outboundEncodedData.position() != 0) {
                    outboundEncodedData.flip();
                    channel.write(outboundEncodedData);
                    outboundEncodedData.compact();

                }

                read = channel.read(inboundEncodedData);
                if (read == -1) {
                    throw new InvalidEventHandlerException("Socket closed");
                }

                if (inboundEncodedData.position() != 0) {
                    inboundEncodedData.flip();
                    engine.unwrap(inboundEncodedData, inboundApplicationData);
                    inboundEncodedData.compact();
                }

                if (inboundApplicationData.position() != 0) {
                    inboundApplicationData.flip();
                    decodedMessageReceiver.accept(inboundApplicationData);
                    inboundApplicationData.compact();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            final boolean idle = read == 0 &&
                    outboundApplicationData.limit() == outboundApplicationData.capacity() &&
                    outboundEncodedData.limit() == outboundEncodedData.capacity() &&
                    inboundApplicationData.limit() == inboundApplicationData.capacity() &&
                    inboundEncodedData.limit() == inboundEncodedData.capacity();

            return !(idle);
        }
    }

    private static final class Handshaker {
        private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;
        private final ByteBuffer applicationData;
        private final ByteBuffer networkData;
        private final ByteBuffer peerApplicationData;
        private final ByteBuffer peerNetworkData;

        Handshaker() {
            this.applicationData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
            this.networkData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
            this.peerApplicationData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
            this.peerNetworkData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        }

        private void performHandshake(final SSLEngine engine, final SocketChannel channel) throws IOException {
            engine.beginHandshake();

            SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
            SSLEngineResult result;

            while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
                    status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                switch (status) {
                    case NEED_UNWRAP:

                        if (channel.read(peerNetworkData) < 0) {
                            throw new IOException("Channel closed");
                        }

                        peerNetworkData.flip();
                        result = engine.unwrap(peerNetworkData, peerApplicationData);
                        peerNetworkData.compact();
                        switch (result.getStatus()) {
                            case OK:
                                break;
                        }
                        break;
                    case NEED_WRAP:
                        networkData.clear();
                        result = engine.wrap(applicationData, networkData);

                        switch (result.getStatus()) {
                            case OK:
                                networkData.flip();
                                while (networkData.hasRemaining()) {
                                    if (channel.write(networkData) < 0) {
                                        throw new IOException("Channel closed");
                                    }
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException(result.getStatus().toString());
                        }
                        break;
                    case NEED_TASK:
                        Runnable delegatedTask;
                        while ((delegatedTask = engine.getDelegatedTask()) != null) {
                            delegatedTask.run();
                        }
                        break;
                }

                status = engine.getHandshakeStatus();
            }
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