package net.openhft.chronicle.network2.event;

import net.openhft.lang.thread.LightPauser;

import java.util.Date;

import static java.util.concurrent.TimeUnit.*;
import static net.openhft.chronicle.network2.event.References.or;

/**
 * Created by peter on 22/01/15.
 */
public class EventGroup implements EventLoop {
    static final long MONITOR_INTERVAL = NANOSECONDS.convert(100, MILLISECONDS);

    final EventLoop monitor = new MonitorEventLoop(new LightPauser(-1, NANOSECONDS.convert(1, SECONDS)));
    final VanillaEventLoop core = new VanillaEventLoop("core", new LightPauser(200000, NANOSECONDS.convert(100, MICROSECONDS)) {
        @Override
        protected void doPause(long maxPauseNS) {
            System.out.println(new Date() + " - pause");
            super.doPause(maxPauseNS);
        }
    });

    public void addHandler(EventHandler handler) {
        switch (or(handler.priority(), HandlerPriority.LOW)) {
            case HIGH:
            case LOW:
            case DAEMON:
                core.addHandler(handler);
                break;
            case MONITOR:
                monitor.addHandler(handler);
                break;
        }
    }

    @Override
    public void start() {
        core.start();
        monitor.start();
        monitor.addHandler(new LoopBlockMonitor());
    }

    @Override
    public void stop() {
        monitor.stop();
        core.stop();
    }

    class LoopBlockMonitor implements EventHandler {
        long lastInterval = 1;

        @Override
        public boolean runOnce() {
            long blockingTime = System.nanoTime() - core.loopStartNS();
            long blockingInterval = blockingTime / (MONITOR_INTERVAL / 2);
            if (blockingInterval > lastInterval) {
                core.dumpRunningState(core.name() + " thread has blocked for " + MILLISECONDS.convert(blockingTime, NANOSECONDS) + " ms.");
            } else {
                lastInterval = Math.max(1, blockingInterval);
            }
            return false;
        }
    }
}
