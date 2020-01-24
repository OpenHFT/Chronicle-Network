package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.network.api.session.SubHandler;

import java.util.Map;

public interface Registerable<T extends SubHandler> {
    Object registryKey();

    void registry(Map<Object, T> registry);
}
