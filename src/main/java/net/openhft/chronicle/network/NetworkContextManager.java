package net.openhft.chronicle.network;

import net.openhft.chronicle.core.annotation.Nullable;

/**
 * @author Rob Austin.
 */
public interface NetworkContextManager<T extends NetworkContext> {

    @Nullable
    T nc();

    void nc(T nc);
}
