package net.openhft.chronicle.network.event;


import net.openhft.chronicle.threads.NamedThreadFactory;

import javax.xml.ws.WebServiceException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Event "Loop" for blocking tasks. Created by peter.lawrey on 26/01/15.
 */
public class BlockingEventLoop implements EventLoop {
    private final EventLoop parent;
    private final ExecutorService service;

    public BlockingEventLoop(EventLoop parent, String name) {
        this.parent = parent;
        service = Executors.newCachedThreadPool(new NamedThreadFactory(name, true));
    }

    @Override
    public void addHandler(EventHandler handler) {
        service.submit(() -> {
            handler.eventLoop(parent);
            while (!handler.isDead())
                handler.runOnce();
        });
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        service.shutdown();
    }

    @Override
    public void close() throws WebServiceException {
        service.shutdown();
        try {
            if (service.awaitTermination(1000, TimeUnit.MILLISECONDS))
                service.shutdownNow();
            service.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            service.shutdownNow();
        }
    }
}
