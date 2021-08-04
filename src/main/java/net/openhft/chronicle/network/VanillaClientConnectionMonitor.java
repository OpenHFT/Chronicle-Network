package net.openhft.chronicle.network;

import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;

import static net.openhft.chronicle.core.Jvm.debug;

public class VanillaClientConnectionMonitor implements ClientConnectionMonitor {

    @Override
    public void onConnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onConnected name=" + name + ",socketAddress=" + socketAddress);
    }

    @Override
    public void onDisconnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onDisconnected name=" + name + ",socketAddress=" + socketAddress);
    }

}
