package net.openhft.chronicle.network.util.proxy;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class TcpProxy extends AbstractCloseable implements Runnable {

    private final int acceptPort;
    private final InetSocketAddress connectAddress;
    private final ExecutorService executorService;
    private final List<ProxyConnection> connections;
    private volatile boolean running;
    private volatile boolean acceptingNewConnections = true;

    public TcpProxy(int acceptPort, InetSocketAddress connectAddress, ExecutorService executorService) {
        this.acceptPort = acceptPort;
        this.connectAddress = connectAddress;
        this.executorService = executorService;
        this.connections = new CopyOnWriteArrayList<>();
    }

    public int getAcceptPort() {
        return acceptPort;
    }

    @Override
    public void run() {
        running = true;
        Jvm.startup().on(TcpProxy.class, "Starting proxy on port " + acceptPort + " proxying to " + connectAddress);
        try (ServerSocketChannel serverSocket = SelectorProvider.provider().openServerSocketChannel()) {
            serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), acceptPort));
            serverSocket.configureBlocking(false);
            while (running) {
                if (acceptingNewConnections) {
                    final SocketChannel newConnection = serverSocket.accept();
                    if (newConnection != null) {
                        final ProxyConnection connection = new ProxyConnection(newConnection, connectAddress);
                        connections.add(connection);
                        executorService.submit(connection);
                    }
                }
                for (int i = 0; i < connections.size(); i++) {
                    if (connections.get(i).isFinished()) {
                        connections.remove(i);
                        i--;
                    }
                }
                Jvm.pause(10);
            }
        } catch (Exception e) {
            Jvm.error().on(TcpProxy.class, "proxy run failed", e);
        }
        Jvm.startup().on(TcpProxy.class, "TCP proxy from " + acceptPort + " proxying to " + connectAddress + " terminated");
    }

    public void dropConnectionsAndPauseNewConnections() {
        acceptingNewConnections = false;
        connections.forEach(AbstractCloseable::close);
    }

    public void dropConnectionsByTimeoutAndPauseNewConnections() {
        acceptingNewConnections = false;
        connections.forEach(ProxyConnection::stopForwardingTraffic);
    }

    public void acceptNewConnections() {
        acceptingNewConnections = true;
    }

    @Override
    protected void performClose() throws IllegalStateException {
        running = false;
        acceptingNewConnections = false;
        connections.forEach(AbstractCloseable::close);
    }
}
