package net.openhft.chronicle.network.internal.lookuptable;

import net.openhft.chronicle.network.HostnamePortLookupTable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

/**
 * Just uses a concurrent map to store the mappings, doesn't work across processes
 */
public class ProcessLocalHostnamePortLookupTable implements HostnamePortLookupTable, java.io.Closeable {

    private final Map<String, InetSocketAddress> aliases;

    public ProcessLocalHostnamePortLookupTable() {
        this.aliases = new ConcurrentSkipListMap<>();
    }

    @Override
    public InetSocketAddress lookup(String description) {
        return aliases.get(description);
    }

    @Override
    public void clear() {
        aliases.clear();
    }

    @Override
    public Set<String> aliases() {
        return aliases.keySet();
    }

    @Override
    public void put(String description, InetSocketAddress address) {
        aliases.put(description, address);
    }

    @Override
    public void forEach(BiConsumer<String, InetSocketAddress> consumer) {
        aliases.forEach(consumer);
    }

    @Override
    public void close() throws IOException {
        clear();
    }
}
