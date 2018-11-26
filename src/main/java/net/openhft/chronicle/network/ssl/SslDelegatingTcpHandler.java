package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * This class is designed to wrap a standard {@see TcpHandler}, providing
 * symmetric encryption/decryption transparently to the underlying handler.
 * <p>
 * When <code>process</code> is called by the {@see TcpEventHandler},
 * this class will first attempt to perform an SSL handshake with the remote
 * connection. This is a blocking operation, and the <code>process</code>
 * call will not return until the handshake is successful, or fails.
 * <p>
 * Further operation is delegated to the {@see SslEngineStateMachine} class,
 * which manages the conversion of data between plain-text and cipher-text
 * either end of the network connection.
 *
 * @param <N> the type of NetworkContext
 */
public final class SslDelegatingTcpHandler<N extends SslNetworkContext>
        implements TcpHandler<N>, NetworkContextManager<N> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslDelegatingTcpHandler.class);

    private final TcpHandler<N> delegate;
    private final BytesBufferHandler<N> bufferHandler = new BytesBufferHandler<>();
    private SslEngineStateMachine stateMachine;
    private boolean handshakeComplete;

    public SslDelegatingTcpHandler(final TcpHandler<N> delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, final N nc) {
        if (!handshakeComplete) {
            try {
                doHandshake(nc);
            } catch (Throwable t) {
                LOGGER.error("Failed to complete SSL handshake at " + Instant.now(), t);
                throw new IllegalStateException("Unable to perform handshake", t);
            }
            handshakeComplete = true;
        }

        bufferHandler.set(delegate, in, out, nc);
        stateMachine.action();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendHeartBeat(final Bytes out, final SessionDetailsProvider sessionDetails) {
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
    public void close() {
        if (stateMachine != null) {
            stateMachine.close();
        }
        delegate.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReadTime(final long readTimeNS, final ByteBuffer inBB, final int position, final int limit) {
        delegate.onReadTime(readTimeNS, inBB, position, limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onWriteTime(final long writeTimeNS,
                            final ByteBuffer byteBuffer,
                            final int position
            , final int limit) {
        delegate.onWriteTime(writeTimeNS, byteBuffer, position, limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReadComplete() {
        delegate.onReadComplete();
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
    public void notifyClosing() {
        delegate.notifyClosing();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public N nc() {
        return (delegate instanceof NetworkContextManager) ? ((NetworkContextManager<N>) delegate).nc() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nc(final N nc) {
        if (delegate instanceof NetworkContextManager) {
            ((NetworkContextManager<N>) delegate).nc(nc);
        }
    }

    private void doHandshake(final N nc) {
        stateMachine = new SslEngineStateMachine(bufferHandler, nc.isAcceptor());
        stateMachine.initialise(nc.sslContext(), nc.socketChannel());
    }
}