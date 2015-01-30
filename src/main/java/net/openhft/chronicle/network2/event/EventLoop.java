package net.openhft.chronicle.network2.event;

/**
 * Created by peter on 22/01/15.
 */
public interface EventLoop {
    void addHandler(EventHandler handler);

    void start();

    void stop();
}
