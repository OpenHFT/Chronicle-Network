package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public final class SslDelegatingTcpHandler<N extends SslNetworkContext> implements TcpHandler<N> {
    private final TcpHandler<N> delegate;
    private final BytesBufferHandler<N> bufferHandler = new BytesBufferHandler<>();
    private SslEngineStateMachine stateMachine;
    private boolean handshakeComplete;

    SslDelegatingTcpHandler(final TcpHandler<N> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, final N nc) {
        if (!handshakeComplete) {
            System.out.printf("%s/%s starting handshake with %s%n",
                    Thread.currentThread().getName(),
                    String.format("%s/0x%s",
                            getClass().getSimpleName(), Integer.toHexString(System.identityHashCode(this))),
                    nc.socketChannel());
            try {
                doHandshake(nc);
            } catch (Throwable t) {
                t.printStackTrace();
            }
            System.out.printf("%s handshake complete%n", String.format("%s/0x%s",
                    getClass().getSimpleName(), Integer.toHexString(System.identityHashCode(this))));
            handshakeComplete = true;
        }

        bufferHandler.set(delegate, in, out, nc);

        stateMachine.action();

        delegate.process(in, out, nc);
    }

    private void doHandshake(final N nc) {
        System.out.printf("%s starting handshake; acceptor: %s%n",
                socketToString(nc.socketChannel()), nc.isAcceptor());
        stateMachine = new SslEngineStateMachine(bufferHandler, nc.isAcceptor());
        stateMachine.initialise(nc.sslContext(), nc.socketChannel());
    }


    private static String socketToString(final SocketChannel channel) {
        return channel.socket().getLocalPort() + "->" +
                ((InetSocketAddress) channel.socket().getRemoteSocketAddress()).getPort();
    }

    @Override
    public void sendHeartBeat(final Bytes out, final SessionDetailsProvider sessionDetails) {
        delegate.sendHeartBeat(out, sessionDetails);
    }

    @Override
    public void onEndOfConnection(final boolean heartbeatTimeOut) {
        delegate.onEndOfConnection(heartbeatTimeOut);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void onReadTime(final long readTimeNS) {
        delegate.onReadTime(readTimeNS);
    }

    @Override
    public void onWriteTime(final long writeTimeNS) {
        delegate.onWriteTime(writeTimeNS);
    }

    @Override
    public void onReadComplete() {
        delegate.onReadComplete();
    }

    @Override
    public boolean hasClientClosed() {
        return delegate.hasClientClosed();
    }

    public static void closeQuietly(@NotNull final Object... closables) {
        Closeable.closeQuietly(closables);
    }

    public static void closeQuietly(@Nullable final Object o) {
        Closeable.closeQuietly(o);
    }

    @Override
    public void notifyClosing() {
        delegate.notifyClosing();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }
}