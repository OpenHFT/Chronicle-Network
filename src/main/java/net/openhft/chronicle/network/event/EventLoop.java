package net.openhft.chronicle.network.event;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public interface EventLoop {
    void addHandler(EventHandler handler);

    void start();

    void stop();
}
