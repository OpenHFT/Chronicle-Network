package net.openhft.chronicle.network.internal;

import java.net.InetSocketAddress;

class TcpInetSocketAddress extends InetSocketAddress {
    private final String toString;

    public TcpInetSocketAddress(String hostname, int port) {
        super(hostname, port);
        toString = super.toString();
    }

    @Override
    public String toString() {
        return toString;
    }
}
