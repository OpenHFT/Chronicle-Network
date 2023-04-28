package net.openhft.chronicle.network.util.proxy;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class ProxyConnection extends AbstractCloseable implements Runnable {

    private final SocketChannel inboundChannel;
    private final InetSocketAddress remoteAddress;
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    private volatile boolean running = true;
    private volatile boolean finished = false;
    private volatile boolean forwardingTraffic = true;

    public ProxyConnection(SocketChannel inboundChannel, InetSocketAddress remoteAddress) {
        this.inboundChannel = inboundChannel;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void run() {
        running = true;
        try (final SocketChannel outboundChannel = SelectorProvider.provider().openSocketChannel()) {
            outboundChannel.configureBlocking(true);
            outboundChannel.connect(remoteAddress);
            Jvm.startup().on(ProxyConnection.class, "Established connection between " + inboundChannel.socket().getRemoteSocketAddress() + " and " + outboundChannel.socket().getRemoteSocketAddress());
            outboundChannel.configureBlocking(false);
            inboundChannel.configureBlocking(false);
            while (running) {
                if (forwardingTraffic) {
                    relayTraffic(inboundChannel, outboundChannel);
                    relayTraffic(outboundChannel, inboundChannel);
                } else {
                    Jvm.pause(10);
                }
            }
            Jvm.startup().on(ProxyConnection.class, "Terminating connection between " + inboundChannel.socket().getRemoteSocketAddress() + " and " + outboundChannel.socket().getRemoteSocketAddress());
        } catch (IOException e) {
            Jvm.error().on(ProxyConnection.class, "Connection failed", e);
        } finally {
            closeQuietly(inboundChannel);
        }
        finished = true;
    }

    private void relayTraffic(SocketChannel sourceChannel, SocketChannel destinationChannel) throws IOException {
        byteBuffer.clear();
        int read = sourceChannel.read(byteBuffer);
        if (read > 0) {
            byteBuffer.flip();
            destinationChannel.write(byteBuffer);
        }
    }

    @Override
    protected void performClose() throws IllegalStateException {
        running = false;
        while (!finished) {
            Jvm.pause(10);
        }
    }

    public boolean isFinished() {
        return finished;
    }

    public void stopForwardingTraffic() {
        forwardingTraffic = false;
    }
}
