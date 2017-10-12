package net.openhft.chronicle.network.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

final class Handshaker {
    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;
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

    void performHandshake(final SSLEngine engine, final SocketChannel channel) throws IOException {
        engine.beginHandshake();

        SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
        SSLEngineResult result;

        while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
                status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (status) {
                case NEED_UNWRAP:

                    if (channel.read(peerNetworkData) < 0) {
                        throw new IOException("Channel closed");
                    }

                    peerNetworkData.flip();
                    result = engine.unwrap(peerNetworkData, peerApplicationData);
                    peerNetworkData.compact();
                    switch (result.getStatus()) {
                        case OK:
                            break;
                    }
                    break;
                case NEED_WRAP:
                    networkData.clear();
                    result = engine.wrap(applicationData, networkData);

                    switch (result.getStatus()) {
                        case OK:
                            networkData.flip();
                            while (networkData.hasRemaining()) {
                                if (channel.write(networkData) < 0) {
                                    throw new IOException("Channel closed");
                                }
                            }
                            break;
                        default:
                            throw new UnsupportedOperationException(result.getStatus().toString());
                    }
                    break;
                case NEED_TASK:
                    Runnable delegatedTask;
                    while ((delegatedTask = engine.getDelegatedTask()) != null) {
                        delegatedTask.run();
                    }
                    break;
            }

            status = engine.getHandshakeStatus();
        }
    }
}
