package net.openhft.chronicle.network.internal;

import net.openhft.lang.io.Bytes;

/**
 * @author Rob Austin.
 */
public interface Actions {

    void setDirty(boolean isDirty);

    void setReceiveHeartbeat(boolean receivesHeartbeat);

    void setSendHeartbeat(boolean sendHeartbeat);

    void close();

    Bytes outWithSize(int size);

}
