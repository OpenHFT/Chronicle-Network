package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class SslTcpHandler<N extends SslNetworkContext> implements TcpHandler<N> {
    private final TcpHandler<N> delegate;
    private ByteBuffer decryptedInput;
    private ByteBuffer plainTextOutput;
    private boolean handshakeRequired = true;

    public SslTcpHandler(final TcpHandler<N> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, final N nc) {
        if (handshakeRequired) {

        }
        // TODO hook into ssl engine here to decrypt
        delegate.process(Bytes.wrapForRead(decryptedInput), Bytes.wrapForWrite(plainTextOutput), nc);
        // TODO hook into ssl engine here to encrypt
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