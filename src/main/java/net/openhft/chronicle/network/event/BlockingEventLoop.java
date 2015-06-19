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
import org.jetbrains.annotations.NotNull;

import javax.xml.ws.WebServiceException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Event "Loop" for blocking tasks. Created by peter.lawrey on 26/01/15.
 */
public class BlockingEventLoop implements EventLoop {
    private final EventLoop parent;
    @NotNull
    private final ExecutorService service;

    public BlockingEventLoop(EventLoop parent, String name) {
        this.parent = parent;
        service = Executors.newCachedThreadPool(new NamedThreadFactory(name, true));
    }

    @Override
    public void addHandler(@NotNull EventHandler handler) {
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
