package net.openhft.chronicle.network2.event;

import net.openhft.lang.thread.NamedThreadFactory;
import net.openhft.lang.thread.Pauser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network2.event.References.or;

/**
 * Created by peter on 22/01/15.
 */
public class VanillaEventLoop implements EventLoop, Runnable {
    final ExecutorService service;

    private final List<EventHandler> highHandlers = new ArrayList<>();
    private final List<EventHandler> lowHandlers = new ArrayList<>();
    private final List<EventHandler> daemonHandlers = new ArrayList<>();
    private final AtomicReference<EventHandler> newHandler = new AtomicReference<>();
    private final Pauser pauser;
    private final String name;
    private long loopStartNS;
    private volatile boolean running = true;
    private volatile Thread thread = null;

    public VanillaEventLoop(String name, Pauser pauser) {
        this.name = name;
        this.pauser = pauser;
        service = Executors.newSingleThreadExecutor(new NamedThreadFactory(name));
    }

    public void start() {
        running = true;
        service.submit(this);
    }

    public void stop() {
        running = false;
    }

    public void addHandler(EventHandler handler) {
        if (thread == null || thread == Thread.currentThread()) {
            addNewHandler(handler);
        } else {
            pauser.unpause();
            while (!newHandler.compareAndSet(null, handler))
                Thread.yield();
        }
    }

    public long loopStartNS() {
        return loopStartNS;
    }

    @Override
    @HotMethod
    public void run() {
        try {
            thread = Thread.currentThread();
            while (running) {
                boolean busy = false;
                for (int i = 0; i < 10; i++) {
                    loopStartNS = System.nanoTime();
                    busy |= runAllHighHandlers();
                    busy |= runOneTenthLowHandler(i);
                }
                acceptNewHandlers();
                if (busy) {
                    pauser.reset();
                } else {
                    runDaemonHandlers();
                    pauser.pause();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @HotMethod
    private boolean runAllHighHandlers() {
        boolean busy = false;
        for (int i = 0; i < highHandlers.size(); i++) {
            EventHandler handler = highHandlers.get(i);
            try {
                busy |= handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                highHandlers.remove(i--);
        }
        return busy;
    }

    @HotMethod
    private boolean runOneTenthLowHandler(int i) {
        boolean busy = false;
        for (int j = i; j < lowHandlers.size(); j += 10) {
            EventHandler handler = lowHandlers.get(i);
            try {
                busy |= handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                lowHandlers.remove(i--);
        }
        return busy;
    }

    @HotMethod
    private void runDaemonHandlers() {
        for (int i = 0; i < daemonHandlers.size(); i++) {
            EventHandler handler = daemonHandlers.get(i);
            try {
                handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                daemonHandlers.remove(i--);
        }
    }

    @HotMethod
    private void acceptNewHandlers() {
        EventHandler handler = newHandler.getAndSet(null);
        if (handler != null) {
            addNewHandler(handler);
        }
    }

    private void addNewHandler(EventHandler handler) {
        switch (or(handler.priority(), HandlerPriority.LOW)) {
            case HIGH:
                highHandlers.add(handler);
                break;
            case LOW:
                lowHandlers.add(handler);
                break;
            case DAEMON:
                daemonHandlers.add(handler);
                break;
        }
        handler.eventLoop(this);
    }

    public String name() {
        return name;
    }

    public void dumpRunningState(String message) {
        StringBuilder out = new StringBuilder(message);
        if (thread == null) {
            out.append("\nbut is null !?");
        } else {
            StackTraceElement[] ste = thread.getStackTrace();
            int last = ste.length - 1;
            for (; last > 0; last--)
                if (!ste[last].getClassName().startsWith("java"))
                    break;
            for (int i = 0; i <= last; i++)
                out.append("\n\tat ").append(ste[i]);
        }
        // TODO use a logger.
        System.out.println(out);
    }
}
