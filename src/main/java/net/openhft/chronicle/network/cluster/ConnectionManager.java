/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.NetworkContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.newSetFromMap;

public class ConnectionManager<T extends NetworkContext<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final Set<ConnectionListener<T>> connectionListeners = newSetFromMap(new IdentityHashMap<>());

    @NotNull
    private final IdentityHashMap<T, AtomicBoolean> isConnected = new IdentityHashMap<>();
    private final Queue<ConnectionListener<T>> newConnectionListeners = new ConcurrentLinkedQueue<>();

    /**
     * Execute any ConnectionChangeListeners that have been added since this was last called
     */
    public void executeAndActivateNewConnectionChangeListeners() {
        while (true) {
            final ConnectionListener<T> connectionListener = newConnectionListeners.poll();
            if (connectionListener == null) {
                break;
            }
            LOGGER.info("Addding and executing new listener {}", System.identityHashCode(connectionListener));
            connectionListeners.add(connectionListener);
            isConnected.forEach((wireOutPublisher, connected) -> connectionListener.onConnectionChange(wireOutPublisher, connected.get()));
        }
    }

    public void addListener(@NotNull ConnectionListener<T> connectionListener) {
        newConnectionListeners.offer(connectionListener);
    }

    public synchronized void onConnectionChanged(boolean isConnected, @NotNull final T nc) {
        @NotNull final Function<T, AtomicBoolean> f = v -> new AtomicBoolean();
        boolean wasConnected = this.isConnected.computeIfAbsent(nc, f).getAndSet(isConnected);
        if (wasConnected != isConnected) connectionListeners.forEach(l -> {
            try {
                l.onConnectionChange(nc, isConnected);
            } catch (IllegalStateException ignore) {
                // this is already logged
            }
        });

    }

    @FunctionalInterface
    public interface ConnectionListener<T extends NetworkContext<T>> {
        /**
         * Callback which is triggered on connection state change: connect or disconnect.
         *
         * @param nc Network context.
         * @param isConnected <tt>true</tt> for connect events, <tt>false</tt> for disconnects.
         */
        void onConnectionChange(T nc, boolean isConnected);
    }
}
