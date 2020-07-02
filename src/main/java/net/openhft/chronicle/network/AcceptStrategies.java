package net.openhft.chronicle.network;

import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

import java.io.IOException;

public enum AcceptStrategies implements AcceptStrategy {
    ACCEPT_ALL {
        @Override
        public ChronicleSocketChannel accept(final ChronicleServerSocketChannel ssc) throws IOException {
            return ssc.accept();
        }
    }
}
