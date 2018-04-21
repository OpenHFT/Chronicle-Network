package net.openhft.chronicle.network.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

final class Handshaker {
    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;
    private static final Logger LOGGER = LoggerFactory.getLogger(Handshaker.class);
    private final ByteBuffer applicationData;
    private final ByteBuffer networkData;
    private final ByteBuffer peerApplicationData;
    private final ByteBuffer peerNetworkData;

    Handshaker() {
        this.applicationData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.networkData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.peerApplicationData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.peerNetworkData = ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
    }

    private static String socketToString(final SocketChannel channel) {
        return channel.socket().getLocalPort() + "->" +
                ((InetSocketAddress) channel.socket().getRemoteSocketAddress()).getPort();
    }

    void performHandshake(final SSLEngine engine, final SocketChannel channel) throws IOException {
        while (!channel.finishConnect()) {
            Thread.yield();
        }

        LOGGER.debug("{} is client: {}", socketToString(channel), engine.getUseClientMode());
        engine.beginHandshake();

        SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
        SSLEngineResult result;

        long underflowCount = 0;
        boolean reportedInitialStatus = false;
        SSLEngineResult.HandshakeStatus lastStatus = status;

        while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
                status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (!reportedInitialStatus) {
                LOGGER.debug("{} initial status {}", socketToString(channel), status);
                reportedInitialStatus = true;
            }
            if (status != lastStatus) {
                LOGGER.debug("{} status change to {}", socketToString(channel), status);
                lastStatus = status;
            }
            switch (status) {
                case NEED_UNWRAP:
                    final int read = channel.read(peerNetworkData);
                    if (read < 0) {
                        throw new IOException("Channel closed");
                    }
                    if (read == 0 && (peerNetworkData.remaining() == 0 ||
                            peerNetworkData.remaining() == peerNetworkData.capacity())) {

                        underflowCount++;
                        continue;
                    }
                    peerNetworkData.flip();
                    final int dataReceived = peerNetworkData.remaining();
                    LOGGER.debug("{} Received {} from handshake peer", socketToString(channel), dataReceived);
                    result = engine.unwrap(peerNetworkData, peerApplicationData);
                    peerNetworkData.compact();
                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_UNDERFLOW:
                            if ((underflowCount & 65535L) == 0L) {
                                LOGGER.debug("Not enough data read from remote end ({})", dataReceived);
                            }
                            underflowCount++;
                            break;
                        default:
                            LOGGER.error("Bad handshake status: {}/{}",
                                    result.getStatus(), result.getHandshakeStatus());
                            break;
                    }
                    break;
                case NEED_WRAP:
                    networkData.clear();
                    result = engine.wrap(applicationData, networkData);

                    switch (result.getStatus()) {
                        case OK:
                            networkData.flip();
                            final int remaining = networkData.remaining();
                            while (networkData.hasRemaining()) {
                                if (channel.write(networkData) < 0) {
                                    throw new IOException("Channel closed");
                                }
                            }
                            LOGGER.debug("{} Wrote {} to handshake peer", socketToString(channel), remaining);
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
                        } catch (RuntimeException e) {
                            LOGGER.error("Delegated task threw exception", e);
                        }
                    }
                    break;
            }

            status = engine.getHandshakeStatus();
        }
    }
}
