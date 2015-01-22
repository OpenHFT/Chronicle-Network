package net.openhft.chronicle.network2.tcp;

import net.openhft.lang.io.Bytes;

/**
 * Created by peter on 22/01/15.
 */
public interface TcpHandler {
    void process(Bytes in, Bytes out);
}
