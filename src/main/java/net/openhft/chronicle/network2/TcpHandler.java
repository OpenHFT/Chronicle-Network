package net.openhft.chronicle.network2;

import net.openhft.lang.io.Bytes;

/**
 * Created by peter on 22/01/15.
 */
public interface TcpHandler {
    void process(Bytes in, Bytes out);
}
