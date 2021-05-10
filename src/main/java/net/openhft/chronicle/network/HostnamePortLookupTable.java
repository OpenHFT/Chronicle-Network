package net.openhft.chronicle.network;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.function.BiConsumer;

public interface HostnamePortLookupTable {

    InetSocketAddress lookup(String description);

    void clear();

    Set<String> aliases();

    void put(String description, InetSocketAddress address);

    void forEach(BiConsumer<String, InetSocketAddress> consumer);
}
