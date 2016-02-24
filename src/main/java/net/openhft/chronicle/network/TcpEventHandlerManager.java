package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.TcpHandler;

/**
 * @author Rob Austin.
 */
public interface TcpEventHandlerManager {
    void tcpHandler(TcpHandler tcpHandler);
}
