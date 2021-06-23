package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import org.jetbrains.annotations.NotNull;

import java.net.SocketAddress;

import static net.openhft.chronicle.core.Jvm.debug;

public class VanillaClientConnectionMonitor extends AbstractCloseable implements ClientConnectionMonitor {

    @Override
    public void onConnected(String name, @NotNull SocketAddress socketAddress) {
        throwExceptionIfClosed();
        debug().on(this.getClass(), "onConnected name=" + name + ",socketAddress=" + socketAddress);
    }

    @Override
    public void onDisconnected(String name, @NotNull SocketAddress socketAddress) {
        throwExceptionIfClosed();
        debug().on(this.getClass(), "onDisconnected name=" + name + ",socketAddress=" + socketAddress);
    }

    @Override
    protected void performClose() throws IllegalStateException {
    }
}
