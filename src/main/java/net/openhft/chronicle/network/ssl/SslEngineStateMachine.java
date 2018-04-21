package net.openhft.chronicle.network.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Instant;

/**
 * This class is responsible for the following:
 * <p>
 * 1. Pass decrypted input data to an underlying TcpHandler, accepting decrypted output data
 * 2. Read encrypted input data from the input buffer and decrypt it for the underlying TcpHandler
 * 3. Encrypt output data from the underlying TcpHandler, and write it to the output buffer
 * <p>
 * <code>initialise</code> is a blocking operation that will only return when a successful
 * SSL handshake has occurred, or if an exception occurred.
 */
final class SslEngineStateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslEngineStateMachine.class);

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
            final BufferHandler bufferHandler, final boolean isAcceptor) {
        this.bufferHandler = bufferHandler;
        this.isAcceptor = isAcceptor;
    }

    void initialise(SSLContext ctx, SocketChannel channel) {
        try {
            channel.configureBlocking(false);
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
            precomputedWrapArray = new ByteBuffer[]{outboundApplicationData};
            precomputedUnwrapArray = new ByteBuffer[]{inboundApplicationData};

            new Handshaker().performHandshake(engine, channel);
        } catch (IOException e) {
            throw new RuntimeException("Unable to perform handshake at " + Instant.now(), e);
        }
    }

    public boolean action() {
        final int read;
        boolean busy = false;
        bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
        try {
            if (outboundApplicationData.position() != 0) {

                outboundApplicationData.flip();

                if (engine.wrap(precomputedWrapArray, outboundEncodedData).
                        getStatus() == SSLEngineResult.Status.CLOSED) {
                    LOGGER.warn("Socket closed");
                    return false;
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
                throw new RuntimeException("Socket closed");
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

    void close() {
        engine.closeOutbound();
    }
}