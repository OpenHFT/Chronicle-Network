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
import net.openhft.chronicle.core.io.ManagedCloseable;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

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
 * @deprecated To be removed in x.25
 *
 * @param <N> the type of NetworkContext
 */
@Deprecated(/* To be removed in x.25 */)
public final class SslDelegatingTcpHandler<N extends SslNetworkContext<N>>
        implements TcpHandler<N>, NetworkContextManager<N> {
    private final TcpHandler<N> delegate;
    private final BytesBufferHandler<N> bufferHandler = new BytesBufferHandler<>();
    private SslEngineStateMachine stateMachine;

    public SslDelegatingTcpHandler(final TcpHandler<N> delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(@NotNull final Bytes<?> in, @NotNull final Bytes<?> out, final N nc) {
        if (delegate instanceof ManagedCloseable)
            ((ManagedCloseable) delegate).throwExceptionIfClosed();

        if (stateMachine == null) {
            stateMachine = new SslEngineStateMachine(bufferHandler, nc.isAcceptor());
            stateMachine.initialise(nc.sslContext());
        }

        bufferHandler.set(delegate, (Bytes<ByteBuffer>) in, (Bytes<ByteBuffer>)out, nc);
        stateMachine.action();
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
    public void close() {
        if (stateMachine != null) {
            stateMachine.close();
        }
        delegate.close();
        bufferHandler.close();
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
}