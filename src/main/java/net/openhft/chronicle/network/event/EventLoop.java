package net.openhft.chronicle.network.event;

import com.sun.xml.internal.ws.Closeable;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public interface EventLoop extends Closeable {
    void addHandler(EventHandler handler);

    void start();

    void stop();

}
