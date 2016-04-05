package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.wire.WriteMarshallable;

/**
 * @author Rob Austin.
 */
public interface WritableSubHandler {

    WriteMarshallable writer();
}
