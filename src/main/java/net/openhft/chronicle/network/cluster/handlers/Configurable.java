package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by Rob Austin
 */
public interface Configurable {
    void configurable(Marshallable config);
}
