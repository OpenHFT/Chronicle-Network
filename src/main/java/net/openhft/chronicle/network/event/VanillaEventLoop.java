/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network.event;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network.event.References.or;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class VanillaEventLoop implements EventLoop, Runnable {
    private final EventLoop parent;
    @NotNull
    private final ExecutorService service;
    private final List<EventHandler> highHandlers = new ArrayList<>();
    private final List<EventHandler> mediumHandlers = new ArrayList<>();
    private final List<EventHandler> timerHandlers = new ArrayList<>();
    private final List<EventHandler> daemonHandlers = new ArrayList<>();
    private final AtomicReference<EventHandler> newHandler = new AtomicReference<>();
    private final Pauser pauser;
    private final long timerIntervalNS;
    private final String name;
    private long loopStartNS;
    private long lastTimerNS;
    private volatile boolean running = true;
    @Nullable
    private volatile Thread thread = null;

    public VanillaEventLoop(EventLoop parent, String name, Pauser pauser, long timerIntervalNS, boolean daemon) {
        this.parent = parent;
        this.name = name;
        this.pauser = pauser;
        this.timerIntervalNS = timerIntervalNS;
        service = Executors.newSingleThreadExecutor(new NamedThreadFactory(name, daemon));
    }

    public void start() {
        running = true;
        service.submit(this);
    }

    public void stop() {
        running = false;
    }

    public void addHandler(@NotNull EventHandler handler) {
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
            int count = 0;
            while (running) {
                boolean busy = false;
                for (int i = 0; i < 10; i++) {
                    loopStartNS = System.nanoTime();
                    busy |= runAllHighHandlers();
                    busy |= runOneTenthLowHandler(i);
                }
                if (lastTimerNS + timerIntervalNS < loopStartNS) {
                    lastTimerNS = loopStartNS;
                    runTimerHandlers();
                }
                acceptNewHandlers();
                if (busy) {
                    System.out.println("b " + count);
                    count = 0;
                    pauser.reset();

                } else {
                    count++;
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
        for (int j = i; j < mediumHandlers.size(); j += 10) {
            EventHandler handler = mediumHandlers.get(j);
            try {
                busy |= handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                mediumHandlers.remove(j--);
        }
        return busy;
    }

    @HotMethod
    private void runTimerHandlers() {
        for (int i = 0; i < timerHandlers.size(); i++) {
            EventHandler handler = timerHandlers.get(i);
            try {
                handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                timerHandlers.remove(i--);
        }
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

    private void addNewHandler(@NotNull EventHandler handler) {
        switch (or(handler.priority(), HandlerPriority.MEDIUM)) {
            case HIGH:
                highHandlers.add(handler);
                break;

            case MEDIUM:
                mediumHandlers.add(handler);
                break;

            case TIMER:
            case DAEMON:
                daemonHandlers.add(handler);
                break;
            default:
                throw new IllegalArgumentException("Cannot add a " + handler.priority() + " task to a busy waiting thread");
        }
        handler.eventLoop(parent);
    }

    public String name() {
        return name;
    }

    public void dumpRunningState(@NotNull String message) {
        Thread thread = this.thread;
        if (thread == null) return;
        StringBuilder out = new StringBuilder(message);
        Jvm.trimStackTrace(out, thread.getStackTrace());
        // TODO use a logger.
        System.out.println(out);
    }

    @Override
    public void close() {
        service.shutdown();
        try {
            if (service.awaitTermination(1000, TimeUnit.MILLISECONDS))
                service.shutdownNow();
            service.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            service.shutdownNow();
        }
    }

    public boolean isAlive() {
        Thread thread = this.thread;
        return thread != null && thread.isAlive();
    }
}
