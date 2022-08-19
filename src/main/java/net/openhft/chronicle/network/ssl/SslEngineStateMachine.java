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

import net.openhft.chronicle.core.io.IORuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
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

    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;

    private final BufferHandler bufferHandler;
    private final boolean isAcceptor;

    private SSLEngine engine;
    private ByteBuffer outboundApplicationData;
    private ByteBuffer outboundEncodedData;
    private ByteBuffer inboundEncodedData;
    private ByteBuffer inboundApplicationData;
    private ByteBuffer[] precomputedWrapArray;
    private ByteBuffer[] precomputedUnwrapArray;

    private boolean inHandshake = true;

    SslEngineStateMachine(final BufferHandler bufferHandler, final boolean isAcceptor) {
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
        SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
        SSLEngineResult result;

        boolean reportedInitialStatus = false;
        SSLEngineResult.HandshakeStatus lastStatus = status;

        boolean didSomething = false;

        while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
                status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (!reportedInitialStatus) {
                LOGGER.debug("initial status {}", status);
                reportedInitialStatus = true;
            }
            if (status != lastStatus) {
                LOGGER.debug("status change to {}", status);
                lastStatus = status;
            }
            switch (status) {
                case NEED_UNWRAP:
                    final int read = bufferHandler.readData(inboundEncodedData);
                    if (read > 0) {
                        didSomething = true;
                    }

                    inboundEncodedData.flip();
                    final int dataReceived = inboundEncodedData.remaining();
                    result = engine.unwrap(inboundEncodedData, outboundApplicationData);
                    if (dataReceived > 0) {
                        LOGGER.debug("Received {} from handshake peer", dataReceived);
                    }
                    inboundEncodedData.compact();

                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_UNDERFLOW:
                            LOGGER.debug("Not enough data read from remote end ({})", dataReceived);
                            break;
                        default:
                            LOGGER.error("Bad handshake status: {}/{}",
                                    result.getStatus(), result.getHandshakeStatus());
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
                            int wrote = bufferHandler.writeData(outboundEncodedData);
                            if (wrote < remaining) {
                                throw new IOException("Handshake message did not fit in buffer");
                            }

                            LOGGER.debug("Wrote {} to handshake peer", wrote);

                            didSomething = true;
                            break;
                        default:
                            throw new UnsupportedOperationException(result.getStatus().toString());
                    }
                    break;
                case NEED_TASK:
                    Runnable delegatedTask;
                    while ((delegatedTask = engine.getDelegatedTask()) != null) {
                        try {
                            delegatedTask.run();
                            LOGGER.debug("Ran task {}", delegatedTask);
                            didSomething = true;
                        } catch (RuntimeException e) {
                            LOGGER.error("Delegated task threw exception", e);
                        }
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

    public boolean action() {
        final int read;
        boolean busy = false;
        try {
            if (inHandshake)
                return performHandshake();

            bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);

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
                throw new IORuntimeException("Socket closed");
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