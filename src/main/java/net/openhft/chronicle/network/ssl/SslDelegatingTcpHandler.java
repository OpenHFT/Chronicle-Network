package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.ManagedCloseable;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This class is designed to wrap a standard {@link TcpHandler}, providing symmetric encryption/decryption transparently to the underlying handler.
 * <p>
 * When <code>process</code> is called by the {@link TcpEventHandler}, this class will first attempt to perform an SSL handshake with the remote
 * connection. This is a blocking operation, and the <code>process</code> call will not return until the handshake is successful, or fails.
 * <p>
 * Further operation is delegated to the {@link SslEngineStateMachine} class, which manages the conversion of data between plain-text and cipher-text
 * either end of the network connection.
 * <p>
 * It is advised to force TLS version 1.2 to be used with this handler, such as by setting system
 * property {@code jdk.tls.server.protocols} to {@code TLSv1.2}.
 *
 * @param <N> the type of NetworkContext
 */
public final class SslDelegatingTcpHandler<N extends SslNetworkContext<N>> extends SimpleCloseable
        implements TcpHandler<N>, NetworkContextManager<N> {
    private static final Bytes<ByteBuffer> EMPTY_APPLICATION_INPUT = Bytes.wrapForRead(ByteBuffer.allocate(0));

    private final Consumer<EventHandler> eventLoopAdder;

    private TcpHandler<N> delegate;
    private ManagedCloseable delegateAsCloseable;

    private Certificate[] peerCertificates;
    private N networkContext;
    private SslEngineStateMachine stateMachine;

    private Bytes<ByteBuffer> input;
    private Bytes<ByteBuffer> output;
    private Bytes<ByteBuffer> decryptedInput;
    private Bytes<ByteBuffer> decryptedOutput;

    public SslDelegatingTcpHandler(Consumer<EventHandler> eventLoopAdder) {
        this.eventLoopAdder = eventLoopAdder;
    }

    public int readData(final ByteBuffer target) {
        final int toRead = Math.min(target.remaining(), (int) input.readRemaining());
        input.read(target);
        return toRead;
    }

    public boolean handleDecryptedData(final ByteBuffer in, final ByteBuffer out) {
        if (peerCertificates == null) {
            peerCertificates = stateMachine.peerCertificates();
            networkContext.peerCertificates(peerCertificates);
        }

        boolean atStart = in.position() == 0 || in.position() == in.limit();
        if (!atStart)
            in.flip();

        prepareBuffers(in, out);

        Bytes<ByteBuffer> applicationInput = decryptedInput;
        if (atStart)
            applicationInput = EMPTY_APPLICATION_INPUT;

        delegate.process(applicationInput, decryptedOutput, networkContext);

        out.position((int) decryptedOutput.writePosition());
        in.position((int) decryptedInput.readPosition());
        return decryptedOutput.writePosition() > 0 || decryptedInput.readRemaining() > 0;
    }

    private void prepareBuffers(ByteBuffer input, ByteBuffer output) {
        if (decryptedInput == null) {
            decryptedInput = wrapAsBytes(input);
        }
        decryptedInput.readLimit(input.limit());
        decryptedInput.readPosition(input.position());

        if (decryptedOutput == null) {
            decryptedOutput = wrapAsBytes(output);
        }
        decryptedOutput.writeLimit(output.limit());
        decryptedOutput.writePosition(output.position());
    }

    private Bytes<ByteBuffer> wrapAsBytes(ByteBuffer bb) {
        BytesStore<?, ?> bs = BytesStore.follow(bb);

        try {
            return (Bytes<ByteBuffer>) bs.bytesForRead();
        } finally {
            bs.release(ReferenceOwner.INIT);
        }
    }

    public int writeData(final ByteBuffer encrypted) {
        if (output.readPosition() != 0) {
            output.compact();
        }
        final int writeRemaining = (int)
                (output.writeRemaining() > Integer.MAX_VALUE ?
                        Integer.MAX_VALUE : output.writeRemaining());
        final int toWrite = Math.min(encrypted.remaining(), writeRemaining);
        output.writeSome(encrypted);
        return toWrite;
    }

    @Override
    protected void performClose() {
        if (decryptedOutput != null)
            decryptedOutput.releaseLast();

        if (decryptedInput != null)
            decryptedInput.releaseLast();

        if (stateMachine != null) {
            stateMachine.close();
        }
        delegate.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(@NotNull final Bytes<?> in, @NotNull final Bytes<?> out, final N nc) {
        if (delegateAsCloseable != null)
            delegateAsCloseable.throwExceptionIfClosed();

        if (stateMachine == null) {
            if (in.unchecked() || out.unchecked())
                throw new IllegalArgumentException("SSL only supports checked buffers, unset tcp.unchecked.buffer");

            stateMachine = new SslEngineStateMachine(this, nc.isAcceptor());
            stateMachine.initialise(nc.sslContext(), nc.sslParameters());
            eventLoopAdder.accept(stateMachine);

            this.decryptedInput = null;
            this.decryptedOutput = null;
        }

        this.input = (Bytes<ByteBuffer>) in;
        this.output = (Bytes<ByteBuffer>) out;
        nc(nc);
        stateMachine.advance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendHeartBeat(final Bytes<?> out, final SessionDetailsProvider sessionDetails) {
        delegate.sendHeartBeat(out, sessionDetails);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onEndOfConnection(final boolean heartbeatTimeOut) {
        delegate.onEndOfConnection(heartbeatTimeOut);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReadTime(long readTimeNS, ByteBuffer inBB, int position, int limit) {
        delegate.onReadTime(readTimeNS, inBB, position, limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onWriteTime(long writeTimeNS, ByteBuffer byteBuffer, int position, int limit) {
        delegate.onWriteTime(writeTimeNS, byteBuffer, position, limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasClientClosed() {
        return delegate.hasClientClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    public TcpHandler<N> delegate() {
        return delegate;
    }

    public SslDelegatingTcpHandler<N> delegate(TcpHandler<N> delegate) {
        this.delegate = delegate;
        this.delegateAsCloseable = delegate instanceof ManagedCloseable ? (ManagedCloseable) delegate : null;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public N nc() {
        return networkContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nc(final N nc) {
        if (nc != networkContext) {
            this.networkContext = nc;

            if (delegate instanceof NetworkContextManager) {
                ((NetworkContextManager<N>) delegate).nc(nc);
            }
        }
    }

    @Override
    public void setOutBufferSupplier(Supplier<Bytes<ByteBuffer>> outBuffer) {
        delegate.setOutBufferSupplier(() -> decryptedOutput);
    }
}