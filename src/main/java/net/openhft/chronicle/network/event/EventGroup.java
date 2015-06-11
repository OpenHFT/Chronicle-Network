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
