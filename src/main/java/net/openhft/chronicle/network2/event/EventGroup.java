package net.openhft.chronicle.network2.event;

import net.openhft.lang.thread.LightPauser;

import static java.util.concurrent.TimeUnit.*;
import static net.openhft.chronicle.network2.event.References.or;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class EventGroup implements EventLoop {
    static final long MONITOR_INTERVAL = NANOSECONDS.convert(100, MILLISECONDS);

    final EventLoop monitor = new MonitorEventLoop(this, new LightPauser(LightPauser.NO_BUSY_PERIOD, NANOSECONDS.convert(1, SECONDS)));
    final VanillaEventLoop core = new VanillaEventLoop(this, "core",
            new LightPauser(NANOSECONDS.convert(20, MICROSECONDS), NANOSECONDS.convert(200, MICROSECONDS)),
            NANOSECONDS.convert(100, MICROSECONDS));
    final BlockingEventLoop blocking = new BlockingEventLoop(this, "blocking");

    public void addHandler(EventHandler handler) {
        switch (or(handler.priority(), HandlerPriority.BLOCKING)) {
            case HIGH:
            case MEDIUM:
            case TIMER:
            case DAEMON:
                core.addHandler(handler);
                break;
            case MONITOR:
                monitor.addHandler(handler);
                break;
            case BLOCKING:
                blocking.addHandler(handler);
                break;
            default:
                throw new IllegalArgumentException("Unknown priority " + handler.priority());
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
