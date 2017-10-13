package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class SslTcpHandler<N extends SslNetworkContext> implements TcpHandler<N> {
    private final TcpHandler<N> delegate;
    private final CopyingBufferHandler bufferHandler = new CopyingBufferHandler();

    private ByteBuffer decryptedInput;
    private ByteBuffer plainTextOutput;
    private boolean handshakeRequired = true;
    private SslEngineStateMachine stateMachine;

    public SslTcpHandler(final TcpHandler<N> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, final N nc) {
        if (handshakeRequired) {
            stateMachine = new SslEngineStateMachine(nc.socketChannel(), bufferHandler, nc.isAcceptor());
            stateMachine.initialise(nc.sslContext());
            handshakeRequired = false;
        }

        bufferHandler.set(in, out);

        stateMachine.action();
        delegate.process(Bytes.wrapForRead(decryptedInput), Bytes.wrapForWrite(plainTextOutput), nc);
        stateMachine.action();
    }

    private static final class CopyingBufferHandler implements BufferHandler {
        private Bytes in;
        private Bytes out;
        private final byte[] wrapper = new byte[1];

        void set(final Bytes in, final Bytes out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public int readData(final ByteBuffer target) throws IOException {
            final long startPosition = in.readPosition();
            in.read(target);

            return (int) (in.readPosition() - startPosition);
        }

        @Override
        public void handleDecryptedData(final ByteBuffer input, final ByteBuffer output) {

        }

        @Override
        public int writeData(final ByteBuffer encrypted) throws IOException {
            final long written = Math.min(encrypted.remaining(), out.writeRemaining());
            for (int i = 0; i < written; i++) {
                wrapper[0] = encrypted.get();
                out.write(wrapper);
            }
            return (int) written;
        }
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