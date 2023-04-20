package net.openhft.chronicle.network.util;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class TestServer extends AbstractCloseable {

    private final ChronicleServerSocketChannel serverSocketChannel;
    private final String uri;
    private Thread serverThread;

    public TestServer(String registryHostName) throws IOException {
        serverSocketChannel = TCPRegistry.createServerSocketChannelFor(registryHostName);
        final InetSocketAddress localSocketAddress = (InetSocketAddress) serverSocketChannel.socket().getLocalSocketAddress();
        uri = localSocketAddress.getHostName() + ":" + localSocketAddress.getPort();
        Jvm.startup().on(TestServer.class, "Test server URI is " + uri);
    }

    public void prepareToAcceptAConnection() {
        CountDownLatch latch = new CountDownLatch(2);
        serverThread = new Thread(() -> {
            waitAtLatch(latch);
            try (final ChronicleSocketChannel accept = serverSocketChannel.accept()) {
                Jvm.startup().on(TestServer.class, "Connected to " + accept.getRemoteAddress());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        serverThread.start();
        waitAtLatch(latch);
    }

    private void waitAtLatch(CountDownLatch latch) {
        try {
            latch.countDown();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(serverSocketChannel);
        try {
            if (serverThread != null)
                serverThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public String uri() {
        return uri;
    }
}
