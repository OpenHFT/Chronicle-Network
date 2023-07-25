package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is responsible for the following:
 * <p>
 * 1. Pass decrypted input data to an underlying {@link net.openhft.chronicle.network.api.TcpHandler}, accepting decrypted output data
 * 2. Read encrypted input data from the input buffer and decrypt it for the underlying TcpHandler
 * 3. Encrypt output data from the underlying TcpHandler, and write it to the output buffer
 * <p>
 * Blocking tasks presented by {@link SSLEngine} will be run on blocking event thread, thus avoiding potential delays on
 * core event loop.
 */
final class SslEngineStateMachine implements EventHandler {
    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;

    private final SslDelegatingTcpHandler<?> handler;
    private final boolean isAcceptor;
    private final AtomicReference<FutureTask<SSLEngineResult.HandshakeStatus>> handshakeTaskFuture = new AtomicReference<>();

    private SSLEngine engine;
    private ByteBuffer outboundApplicationData;
    private ByteBuffer outboundEncodedData;
    private ByteBuffer inboundEncodedData;
    private ByteBuffer inboundApplicationData;
    private ByteBuffer[] precomputedWrapArray;
    private ByteBuffer[] precomputedUnwrapArray;

    private boolean inHandshake = true;
    private boolean socketClosed = false;

    SslEngineStateMachine(final SslDelegatingTcpHandler<?> handler, final boolean isAcceptor) {
        this.handler = handler;
        this.isAcceptor = isAcceptor;
    }

    void initialise(SSLContext ctx, SSLParameters sslParameters) {
        try {
            engine = ctx.createSSLEngine();
            engine.setUseClientMode(!isAcceptor);
            if (sslParameters != null) {
                engine.setSSLParameters(sslParameters);
            } else if (isAcceptor) {
                engine.setWantClientAuth(true);
                engine.setNeedClientAuth(true);
            }
            engine.beginHandshake();

            outboundApplicationData = ByteBuffer.allocateDirect(Math.max(engine.getSession().getApplicationBufferSize(), HANDSHAKE_BUFFER_CAPACITY));
            outboundEncodedData = ByteBuffer.allocateDirect(Math.max(engine.getSession().getPacketBufferSize(), HANDSHAKE_BUFFER_CAPACITY));
            inboundApplicationData = ByteBuffer.allocateDirect(Math.max(engine.getSession().getApplicationBufferSize(), HANDSHAKE_BUFFER_CAPACITY));
            inboundEncodedData = ByteBuffer.allocateDirect(Math.max(engine.getSession().getPacketBufferSize(), HANDSHAKE_BUFFER_CAPACITY));
            // eliminates array creation on each call to SSLEngine.wrap()
            precomputedWrapArray = new ByteBuffer[]{outboundApplicationData};
            precomputedUnwrapArray = new ByteBuffer[]{inboundApplicationData};
        } catch (IOException e) {
            throw new RuntimeException("Unable to perform handshake at " + Instant.now(), e);
        }
    }

    boolean performHandshake() throws IOException {
        SSLEngineResult.HandshakeStatus status;

        FutureTask<SSLEngineResult.HandshakeStatus> handshakeTask = handshakeTaskFuture.get();
        if (handshakeTask != null) {
            if (handshakeTask.isDone()) {
                try {
                    status = handshakeTask.get();
                    Jvm.debug().on(SslEngineStateMachine.class, "Task finished");
                } catch (ExecutionException ex) {
                    Jvm.error().on(SslEngineStateMachine.class, "Delegated task threw exception", ex);
                    throw new RuntimeException(ex);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    Jvm.error().on(SslEngineStateMachine.class, "Delegated task threw exception", ex);
                    throw new RuntimeException(ex);
                } finally {
                    handshakeTaskFuture.set(null);
                }
            } else {
                return false;
            }
        } else {
            status = engine.getHandshakeStatus();
        }

        SSLEngineResult result;

        boolean reportedInitialStatus = false;
        SSLEngineResult.HandshakeStatus lastStatus = status;

        boolean didSomething = false;

        while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
                status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (!reportedInitialStatus) {
                Jvm.debug().on(SslEngineStateMachine.class, String.format("initial status %s", status));
                reportedInitialStatus = true;
            }
            if (status != lastStatus) {
                Jvm.debug().on(SslEngineStateMachine.class, String.format("status change to %s", status));
                lastStatus = status;
            }
            switch (status) {
                case NEED_UNWRAP:
                    final int read = handler.readData(inboundEncodedData);
                    if (read > 0) {
                        didSomething = true;
                    }

                    inboundEncodedData.flip();
                    final int dataReceived = inboundEncodedData.remaining();
                    result = engine.unwrap(inboundEncodedData, outboundApplicationData);
                    if (dataReceived > 0)
                        Jvm.debug().on(SslEngineStateMachine.class, String.format("Received %s from handshake peer", dataReceived));

                    inboundEncodedData.compact();

                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_UNDERFLOW:
                            Jvm.debug().on(SslEngineStateMachine.class, String.format("Not enough data read from remote end (%s)", dataReceived));
                            break;
                        default:
                            Jvm.error().on(SslEngineStateMachine.class, String.format("Bad handshake status: %s/%s",
                                    result.getStatus(), result.getHandshakeStatus()));
                            break;
                    }
                    break;
                case NEED_WRAP:
                    outboundEncodedData.clear();
                    result = engine.wrap(outboundApplicationData, outboundEncodedData);

                    switch (result.getStatus()) {
                        case OK:
                            outboundEncodedData.flip();
                            int remaining = outboundEncodedData.remaining();
                            int wrote = handler.writeData(outboundEncodedData);
                            if (wrote < remaining) {
                                throw new IOException("Handshake message did not fit in buffer");
                            }

                            Jvm.debug().on(SslEngineStateMachine.class, String.format("Wrote %s to handshake peer", wrote));

                            didSomething = true;
                            break;
                        case CLOSED:
                            socketClosed = true;
                            return false;
                        default:
                            throw new UnsupportedOperationException(result.getStatus().toString());
                    }
                    break;
                case NEED_TASK:
                    Runnable delegatedTask = engine.getDelegatedTask();
                    if (delegatedTask != null) {
                        handshakeTaskFuture.set(new FutureTask<>(delegatedTask, SSLEngineResult.HandshakeStatus.NEED_TASK));
                        Jvm.debug().on(SslEngineStateMachine.class, String.format("Scheduled task %s", delegatedTask));
                        didSomething = true;
                    }
                    break;
            }

            status = engine.getHandshakeStatus();

            if (!didSomething)
                return false;

            didSomething = false;
        }

        outboundApplicationData.clear();
        outboundApplicationData.clear();
        outboundEncodedData.clear();
        inHandshake = false;

        return true;
    }

    public Certificate[] peerCertificates() {
        try {
            return engine.getSession().getPeerCertificates();
        } catch (SSLPeerUnverifiedException ex) {
            return new Certificate[0];
        }
    }

    private enum Op {
        WRITE, HANDLE, READ_THEN_HANDLE;
        public static final Op[] OPS = new Op[] {WRITE, HANDLE, WRITE, READ_THEN_HANDLE, WRITE};
    }

    public boolean advance() {
        int read;
        boolean busy = false;
        try {
            if (socketClosed)
                return false;

            if (inHandshake)
                return performHandshake();

            for (Op op : Op.OPS) {
                switch (op) {
                    case WRITE:
                        if (outboundApplicationData.position() > 0 && outboundApplicationData.position() != outboundApplicationData.capacity())
                            outboundApplicationData.flip();

                        if (outboundApplicationData.limit() != 0 && outboundApplicationData.limit() != outboundApplicationData.capacity()) {
                            if (engine.wrap(precomputedWrapArray, outboundEncodedData).getStatus() == SSLEngineResult.Status.CLOSED) {
                                Jvm.warn().on(SslEngineStateMachine.class, "Socket closed");
                                socketClosed = true;
                                return false;
                            }

                            outboundApplicationData.compact();
                        }

                        if (outboundEncodedData.position() != 0) {
                            outboundEncodedData.flip();
                            handler.writeData(outboundEncodedData);
                            busy |= outboundEncodedData.hasRemaining();
                            outboundEncodedData.compact();
                        }
                        break;

                    case HANDLE:
                        boolean needCompact = inboundApplicationData.position() != 0;
                        busy |= handler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
                        if (needCompact)
                            inboundApplicationData.compact();
                        break;

                    case READ_THEN_HANDLE:
                        read = handler.readData(inboundEncodedData);
                        // Why different handling?
                        if (read == -1)
                            throw new IORuntimeException("Socket closed");

                        busy |= read != 0;

                        if (inboundEncodedData.position() != 0) {
                            inboundEncodedData.flip();
                            engine.unwrap(inboundEncodedData, precomputedUnwrapArray);
                            busy |= inboundEncodedData.hasRemaining();
                            inboundEncodedData.compact();
                        }

                        if (inboundApplicationData.position() != 0) {
                            busy |= handler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
                            inboundApplicationData.compact();
                        }
                        break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return busy;
    }

    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    public boolean action() {
        FutureTask<SSLEngineResult.HandshakeStatus> handshakeTask = handshakeTaskFuture.get();
        if (handshakeTask != null && !handshakeTask.isDone())
            handshakeTask.run();

        return false;
    }

    void close() {
        engine.closeOutbound();
    }
}