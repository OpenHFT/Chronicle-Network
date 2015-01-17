package net.openhft.chronicle.network.internal;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: peter.lawrey Date: 18/08/13 Time: 11:37
 */
public class NamedThreadFactory implements ThreadFactory {
    private final AtomicInteger id = new AtomicInteger();
    private final String name;
    private final Boolean daemon;

    public NamedThreadFactory(@NotNull String name) {
        this(name, null);
    }

    public NamedThreadFactory(@NotNull String name, Boolean daemon) {
        this.name = name;
        this.daemon = daemon;
    }

    @NotNull
    @Override
    public Thread newThread(@NotNull Runnable r) {
        int id = this.id.getAndIncrement();
        String nameN = id == 0 ? name : (name + '-' + id);
        Thread t = new Thread(r, nameN);
        if (daemon != null)
            t.setDaemon(daemon);
        return t;
    }
}
