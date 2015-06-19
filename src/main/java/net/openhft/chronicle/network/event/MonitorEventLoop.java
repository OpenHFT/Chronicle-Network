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

import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import javax.xml.ws.WebServiceException;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public class MonitorEventLoop implements EventLoop, Runnable, Closeable {
    final ExecutorService service = Executors.newSingleThreadExecutor(new NamedThreadFactory("event-loop-monitor", true));

    private final EventLoop parent;
    private final List<EventHandler> handlers = new ArrayList<>();
    private final Pauser pauser;
    private volatile boolean running = true;

    public MonitorEventLoop(EventLoop parent, Pauser pauser) {
        this.parent = parent;
        this.pauser = pauser;
    }

    public void start() {
        running = true;
        service.submit(this);
    }

    public void stop() {
        running = false;
    }

    public void addHandler(@NotNull EventHandler handler) {
        synchronized (handler) {
            handlers.add(handler);
            handler.eventLoop(parent);
        }
    }

    @Override
    @HotMethod
    public void run() {
        try {
            while (running) {
                boolean busy;
                synchronized (handlers) {
                    busy = runHandlers();
                }
                if (busy) {
                    pauser.reset();

                } else {
                    pauser.pause();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @HotMethod
    private boolean runHandlers() {
        boolean busy = false;
        for (int i = 0; i < handlers.size(); i++) {
            EventHandler handler = handlers.get(i);
            try {
                busy |= handler.runOnce();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (handler.isDead())
                handlers.remove(i--);
        }
        return busy;
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
