package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.NetworkContext;

import javax.net.ssl.SSLContext;

public interface SslNetworkContext<T extends NetworkContext<T>> extends NetworkContext<T> {
    SSLContext sslContext();
}