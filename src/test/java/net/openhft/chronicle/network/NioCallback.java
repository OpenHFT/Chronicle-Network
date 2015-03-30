package net.openhft.chronicle.network;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;

/**
 * @author Rob Austin.
 */
public interface NioCallback {

    enum EventType {WRITE, READ, ACCEPT, CONNECT, CLOSED}

    /**
     * called when there is a NIO Event
     */
    void onEvent(@NotNull Bytes in, @NotNull Bytes out, @NotNull EventType eventType);

}
