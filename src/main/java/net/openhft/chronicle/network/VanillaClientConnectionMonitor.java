package net.openhft.chronicle.network;

import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;

import static net.openhft.chronicle.core.Jvm.debug;

public class VanillaClientConnectionMonitor extends AbstractMarshallableCfg implements ClientConnectionMonitor {

    @Override
    public void onConnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onConnected name=" + name + ",socketAddress=" + socketAddress);
    }

    @Override
    public void onDisconnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onDisconnected name=" + name + ",socketAddress=" + socketAddress);
    }

}
