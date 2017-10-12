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

    public static void main(String[] args) throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(null);
        serverChannel.configureBlocking(true);

        final SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress("127.0.0.1", serverChannel.socket().getLocalPort()));

        final Client client = new Client();

        threadPool.submit(new StateMachineProcessor(channel, false,
                client::onReceivedMessage, client::onAddApplicationData, getInitialisedContext()));

        final SocketChannel serverConnection = serverChannel.accept();
        serverConnection.configureBlocking(false);

        final Server server = new Server();
        threadPool.submit(new StateMachineProcessor(serverConnection, true,
                server::onReceivedMessage, server::onAddApplicationData, getInitialisedContext()));
    }

    private static final class StateMachineProcessor implements Runnable {
        private final SocketChannel channel;
        private final boolean isAcceptor;
        private final Consumer<ByteBuffer> decodedMessageReceiver;
        private final Consumer<ByteBuffer> applicationDataPopulator;
        private final SSLContext context;

        StateMachineProcessor(final SocketChannel channel, final boolean isAcceptor,
                              final Consumer<ByteBuffer> decodedMessageReceiver,
                              final Consumer<ByteBuffer> applicationDataPopulator, final SSLContext context) {
            this.channel = channel;
            this.isAcceptor = isAcceptor;
            this.decodedMessageReceiver = decodedMessageReceiver;
            this.applicationDataPopulator = applicationDataPopulator;
            this.context = context;
        }

        @Override
        public void run() {
            try {
                while (!channel.finishConnect()) {
                    Thread.yield();
                }

                final SslEngineStateMachine stateMachine =
                        new SslEngineStateMachine(channel, decodedMessageReceiver, applicationDataPopulator, isAcceptor);
                stateMachine.initialise(context);

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
    }

    private static final class Client {
        private int counter = 0;

        private void onAddApplicationData(final ByteBuffer outboundApplicationData) {
            outboundApplicationData.clear();
            outboundApplicationData.put(("hello " + (counter++)).getBytes(StandardCharsets.US_ASCII));
        }

        private void onReceivedMessage(final ByteBuffer buffer) {
            final byte[] tmp = new byte[buffer.remaining()];
            buffer.get(tmp);
            System.out.println(new String(tmp, StandardCharsets.UTF_8));
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


    private static final class SslEngineStateMachine implements EventHandler {
        private final SocketChannel channel;
        private final Consumer<ByteBuffer> decodedMessageReceiver;
        private final Consumer<ByteBuffer> applicationDataPopulator;
        private final boolean isAcceptor;
        private SSLEngine engine;
        private ByteBuffer outboundApplicationData;
        private ByteBuffer outboundEncodedData;
        private ByteBuffer inboundEncodedData;
        private ByteBuffer inboundApplicationData;

        private SslEngineStateMachine(
                final SocketChannel channel,
                final Consumer<ByteBuffer> decodedMessageReceiver,
                final Consumer<ByteBuffer> applicationDataPopulator,
                final boolean isAcceptor) {
            this.channel = channel;
            this.decodedMessageReceiver = decodedMessageReceiver;
            this.applicationDataPopulator = applicationDataPopulator;
            this.isAcceptor = isAcceptor;
        }

        void initialise(SSLContext ctx) {
            try {

                engine = ctx.createSSLEngine();
                engine.setUseClientMode(!isAcceptor);
                if (isAcceptor) {
                    engine.setNeedClientAuth(true);
                }
                outboundApplicationData = ByteBuffer.allocateDirect(engine.getSession().getApplicationBufferSize());
                outboundEncodedData = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
                inboundApplicationData = ByteBuffer.allocateDirect(engine.getSession().getApplicationBufferSize());
                inboundEncodedData = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());

                new Handshaker().performHandshake(engine, channel);
            } catch (IOException e) {
                throw new RuntimeException("Unable to perform handshake", e);
            }
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InterruptedException {
            final int read;
            boolean busy = false;
            applicationDataPopulator.accept(outboundApplicationData);
            try {
                if (outboundApplicationData.position() != 0) {

                    outboundApplicationData.flip();
                    if (engine.wrap(outboundApplicationData, outboundEncodedData).
                            getStatus() == SSLEngineResult.Status.CLOSED) {
                        throw new InvalidEventHandlerException("Socket closed");
                    }
                    busy |= outboundApplicationData.hasRemaining();
                    outboundApplicationData.compact();
                }
                if (outboundEncodedData.position() != 0) {
                    outboundEncodedData.flip();
                    channel.write(outboundEncodedData);
                    busy |= outboundEncodedData.hasRemaining();
                    outboundEncodedData.compact();
                }

                read = channel.read(inboundEncodedData);
                if (read == -1) {
                    throw new InvalidEventHandlerException("Socket closed");
                }
                busy |= read != 0;

                if (inboundEncodedData.position() != 0) {
                    inboundEncodedData.flip();
                    engine.unwrap(inboundEncodedData, inboundApplicationData);
                    busy |= inboundEncodedData.hasRemaining();
                    inboundEncodedData.compact();
                }

                if (inboundApplicationData.position() != 0) {
                    inboundApplicationData.flip();
                    decodedMessageReceiver.accept(inboundApplicationData);
                    busy |= inboundApplicationData.hasRemaining();
                    inboundApplicationData.compact();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return busy;
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