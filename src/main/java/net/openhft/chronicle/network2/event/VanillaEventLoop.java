/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Created by peter.lawrey on 22/01/15.
 */
public class VanillaEventLoop implements EventLoop, Runnable {
    private final EventLoop parent;
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
    private volatile Thread thread = null;

    public VanillaEventLoop(EventLoop parent, String name, Pauser pauser, long timerIntervalNS) {
        this.parent = parent;
        this.name = name;
        this.pauser = pauser;
        this.timerIntervalNS = timerIntervalNS;
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
            EventHandler handler = mediumHandlers.get(i);
            try {
                busy |= handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                mediumHandlers.remove(i--);
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

    private void addNewHandler(EventHandler handler) {
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
