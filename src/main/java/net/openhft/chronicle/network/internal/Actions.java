package net.openhft.chronicle.network.internal;

/**
 * @author Rob Austin.
 */
public interface Actions {

    void setDirty(boolean isDirty);

    void setReceiveHeartbeat(boolean receivesHeartbeat);

    void setSendHeartbeat(boolean sendHeartbeat);

    void close();
}
