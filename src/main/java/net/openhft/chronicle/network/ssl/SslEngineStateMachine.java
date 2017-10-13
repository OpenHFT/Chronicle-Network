package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

final class SslEngineStateMachine implements EventHandler {
    private final SocketChannel channel;
    private final BufferHandler bufferHandler;
    private final boolean isAcceptor;

    private SSLEngine engine;
    private ByteBuffer outboundApplicationData;
    private ByteBuffer outboundEncodedData;
    private ByteBuffer inboundEncodedData;
    private ByteBuffer inboundApplicationData;
    private ByteBuffer[] precomputedWrapArray;
    private ByteBuffer[] precomputedUnwrapArray;

    SslEngineStateMachine(
            final SocketChannel channel,
            final BufferHandler bufferHandler, final boolean isAcceptor) {
        this.channel = channel;
        this.bufferHandler = bufferHandler;
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
            // eliminates array creation on each call to SSLEngine.wrap()
            precomputedWrapArray = new ByteBuffer[] {outboundApplicationData};
            precomputedUnwrapArray = new ByteBuffer[] {inboundApplicationData};

            new Handshaker().performHandshake(engine, channel);
        } catch (IOException e) {
            throw new RuntimeException("Unable to perform handshake", e);
        }
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        final int read;
        boolean busy = false;
        bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
        try {
            if (outboundApplicationData.position() != 0) {

                outboundApplicationData.flip();

                if (engine.wrap(precomputedWrapArray, outboundEncodedData).
                        getStatus() == SSLEngineResult.Status.CLOSED) {
                    throw new InvalidEventHandlerException("Socket closed");
                }
                busy = outboundApplicationData.hasRemaining();
                outboundApplicationData.compact();
            }
            if (outboundEncodedData.position() != 0) {
                outboundEncodedData.flip();
                bufferHandler.writeData(outboundEncodedData);
                busy |= outboundEncodedData.hasRemaining();
                outboundEncodedData.compact();
            }

            read = bufferHandler.readData(inboundEncodedData);
            if (read == -1) {
                throw new InvalidEventHandlerException("Socket closed");
            }
            busy |= read != 0;

            if (inboundEncodedData.position() != 0) {
                inboundEncodedData.flip();
                engine.unwrap(inboundEncodedData, precomputedUnwrapArray);
                busy |= inboundEncodedData.hasRemaining();
                inboundEncodedData.compact();
            }

            if (inboundApplicationData.position() != 0) {
                inboundApplicationData.flip();
                bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
                busy |= inboundApplicationData.hasRemaining();
                inboundApplicationData.compact();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return busy;
    }
}