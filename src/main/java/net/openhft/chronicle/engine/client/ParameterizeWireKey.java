package net.openhft.chronicle.engine.client;

import net.openhft.chronicle.wire.WireKey;

/**
 * Created by Rob Austin
 */
public interface ParameterizeWireKey extends WireKey {
    <P extends WireKey> P[] params();
}
