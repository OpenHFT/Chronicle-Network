package net.openhft.chronicle.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public enum AcceptStrategies implements AcceptStrategy {
    ACCEPT_ALL {
        @Override
        public SocketChannel accept(ServerSocketChannel ssc) throws IOException {
            return ssc.accept();
        }
    }
}
