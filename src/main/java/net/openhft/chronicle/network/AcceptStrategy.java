package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.Marshallable;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Can be used to reject incoming connections e.g. you could implement an AcceptStrategy that checks remote IPs
 * and rejects if not in whitelist
 */
@FunctionalInterface
public interface AcceptStrategy extends Marshallable {

    AcceptStrategy ACCEPT_ALL = ssc -> ssc.accept();

    /**
     * Determine whether to accept the incoming connection
     *
     * @param ssc
     * @return null to reject the connection, otherwise return the accepted SocketChannel
     * @throws IOException
     */
    SocketChannel accept(ServerSocketChannel ssc) throws IOException;
}
