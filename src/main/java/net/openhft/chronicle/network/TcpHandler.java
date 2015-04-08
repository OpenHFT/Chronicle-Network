package net.openhft.chronicle.network;


import net.openhft.chronicle.bytes.Bytes;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public interface TcpHandler {
    void process(Bytes in, Bytes out);
}
