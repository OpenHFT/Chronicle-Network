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

import net.openhft.chronicle.threads.LightPauser;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.*;
import static net.openhft.chronicle.network.event.References.or;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class EventGroup implements EventLoop  {
    static final long MONITOR_INTERVAL = NANOSECONDS.convert(100, MILLISECONDS);

    public static boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().
            getInputArguments().toString().indexOf("jdwp") >= 0;

    final EventLoop monitor = new MonitorEventLoop(this, new LightPauser(LightPauser.NO_BUSY_PERIOD, NANOSECONDS.convert(1, SECONDS)));
    final VanillaEventLoop core;
    final BlockingEventLoop blocking = new BlockingEventLoop(this, "blocking-event-loop");

    public EventGroup(boolean daemon) {
        core = new VanillaEventLoop(this, "core-event-loop",
                new LightPauser(NANOSECONDS.convert(20, MICROSECONDS), NANOSECONDS.convert(200, MICROSECONDS)),
                NANOSECONDS.convert(100, MICROSECONDS), daemon);
    }

    public void addHandler(@NotNull EventHandler handler) {
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

    @Override
    public void close() throws IOException {
        stop();
        monitor.close();
        blocking.close();
        core.close();
    }

    class LoopBlockMonitor implements EventHandler {
        long lastInterval = 1;

        @Override
        public boolean runOnce() {
            long blockingTime = System.nanoTime() - core.loopStartNS();
            long blockingInterval = blockingTime / (MONITOR_INTERVAL / 2);

            if (blockingInterval > lastInterval && !IS_DEBUG && core.isAlive()) {
                core.dumpRunningState(core.name() + " thread has blocked for " + MILLISECONDS.convert(blockingTime, NANOSECONDS) + " ms.");

            } else {
                lastInterval = Math.max(1, blockingInterval);
            }
            return false;
        }
    }
}
