package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface ClientConnectionMonitor {

    /**
     * Call just after the client as successfully established a connection to the server
     *
     * @param name          the name of the connection
     * @param socketAddress the address that we have just connected to
     */
    void onConnected(@Nullable String name, @NotNull SocketAddress socketAddress);


    /**
     * call just after the client has disconnect to the server, this maybe called as part of a
     * failover
     *
     * @param name          the name of the connection
     * @param socketAddress the address of the socket that we have been disconnected from
     */
    void onDisconnected(@Nullable String name, @NotNull SocketAddress socketAddress);
}
