package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.wire.WireOut;

@FunctionalInterface
public interface WireOutConsumer {
    void accept(WireOut wireOut) throws InvalidEventHandlerException, InterruptedException;
}

    