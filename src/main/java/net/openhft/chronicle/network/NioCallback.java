package net.openhft.chronicle.network;

import net.openhft.lang.io.Bytes;

/**
 * @author Rob Austin.
 */
public interface NioCallback {

    /**
     * called when a client establish a connection with a server
     */
    void onConnect();

    /**
     * called whenever a server receive a connection from a client
     */
    void onAccept();


    /**
     * called to allow you to write data, if you have finished writing data the you should {@code
     * setDirty(false)}
     *
     * @see net.openhft.chronicle.network.internal.Actions#setDirty(boolean)
     */
    void onWrite(Bytes out);

    /**
     * called when there is data to read
     */
    void onRead(Bytes in);

    /**
     * an opportunity to clean up, as the socket connection has closed It the connection was dropped
     * due to a heartbeat failure, the connection will be re-establish automatically, as such the
     * {@code NioCallbackFactory} will be called.
     */
    void onClose();
}
