package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.Marshallable;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Can be used to reject incoming connections e.g. you could implement an AcceptStrategy that checks remote IPs
 * and rejects if not in whitelist
 *
 * Created by Jerry Shea on 29/08/18.
 */
public interface AcceptStrategy extends Marshallable {

    AcceptStrategy ACCEPT_ALL = ssc -> ssc.accept();

    SocketChannel accept(ServerSocketChannel ssc) throws IOException;
}
