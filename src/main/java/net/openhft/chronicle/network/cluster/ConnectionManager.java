/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionManager<T extends NetworkContext<T>> {

    private static final int EMPTY_SEQUENCE = -1;
    private final List<ConnectionListenerHolder<T>> connectionListeners = new ArrayList<>();
    private final AtomicInteger lastListenerAddedSequence = new AtomicInteger(EMPTY_SEQUENCE);

    public synchronized void addListener(@NotNull ConnectionListener<T> connectionListener) {
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < connectionListeners.size(); i++) {
            if (connectionListeners.get(i).connectionListener() == connectionListener) {
                return;
            }
        }
        connectionListeners.add(new ConnectionListenerHolder<>(lastListenerAddedSequence.incrementAndGet(), connectionListener));
    }

    /**
     * Execute any new connection listeners that have been added since the emitter last emitted
     * a connection event.
     *
     * @param nc    The network context of the emitter
     * @param token The event emitter token
     */
    public void executeNewListeners(@NotNull final T nc, @NotNull EventEmitterToken token) {
        assert token != null : "Only emitters who've already emitted should call executeNewListeners";
        if (lastListenerAddedSequence.get() > token.latestSequenceExecuted) {
            executeListenersWithSequenceGreaterThan(token.latestSequenceExecuted, nc, token);
        }
    }

    /**
     * The connection state of the network context changed, notify all listeners
     * <p>
     * Idempotent if the same emitter calls with the same state consecutively.
     *
     * @param isConnected The new connection state
     * @param nc          The network context
     * @param token       The event emitter token, or null if the event emitter is new
     * @return The event emitter token that should be used going forward
     */
    public EventEmitterToken onConnectionChanged(boolean isConnected,
                                                 @NotNull final T nc,
                                                 @Nullable final EventEmitterToken token) {
        final EventEmitterToken tokenToUse = (token == null ? new EventEmitterToken() : token);
        if (tokenToUse.connected.compareAndSet(!isConnected, isConnected)) {
            executeListenersWithSequenceGreaterThan(EMPTY_SEQUENCE, nc, tokenToUse);
        }
        return tokenToUse;
    }

    private synchronized void executeListenersWithSequenceGreaterThan(int lowerSequenceLimit,
                                                                      @NotNull final T nc,
                                                                      @NotNull EventEmitterToken token) {
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < connectionListeners.size(); i++) {
            ConnectionListenerHolder<T> l = connectionListeners.get(i);
            if (l.sequence > lowerSequenceLimit) {
                try {
                    l.connectionListener.onConnectionChange(nc, token.connected.get());
                } catch (IllegalStateException ignore) {
                    // this is already logged
                }
                token.latestSequenceExecuted = Math.max(token.latestSequenceExecuted, l.sequence);
            }
        }
    }

    @FunctionalInterface
    public interface ConnectionListener<T extends NetworkContext<T>> {
        /**
         * Callback which is triggered on connection state change: connect or disconnect.
         *
         * @param nc          Network context.
         * @param isConnected <code>true</code> for connect events, <code>false</code> for disconnects.
         */
        void onConnectionChange(T nc, boolean isConnected);
    }

    /**
     * A connection listener that knows when it was added
     */
    private static final class ConnectionListenerHolder<C extends NetworkContext<C>> {
        private final int sequence;
        private final ConnectionListener<C> connectionListener;

        public ConnectionListenerHolder(int sequence, @NotNull ConnectionListener<C> connectionListener) {
            this.sequence = sequence;
            this.connectionListener = connectionListener;
        }

        public ConnectionListener<C> connectionListener() {
            return connectionListener;
        }
    }

    /**
     * An <strong>opaque</strong> token that NetworkContext connection event emitters need to retain and
     * provide when they dispatch events.
     * <p>
     * Keeps track of the listeners they have executed and what the connected state was
     * on their previous call.
     * <p>
     * Allows this state to immediately available, and be garbage collected with the emitter.
     */
    public static final class EventEmitterToken {
        private final AtomicBoolean connected = new AtomicBoolean(false);
        private volatile int latestSequenceExecuted = Integer.MIN_VALUE;

        /**
         * These should only be created by the ConnectionManager
         */
        private EventEmitterToken() {
        }
    }
}
