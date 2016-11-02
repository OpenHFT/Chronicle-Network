/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * @author Rob Austin.
 */
class ClusterNotifier implements TerminationEventHandler, ConnectionChangedNotifier {

    private final List<WriteMarshallable> bootstaps;
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final ConnectionChangedNotifier connectionManager;
    private final HostConnector hostConnector;

    ClusterNotifier(ConnectionChangedNotifier connectionManager,
                    HostConnector hostConnector,
                    List<WriteMarshallable> bootstaps) {
        this.connectionManager = connectionManager;
        this.hostConnector = hostConnector;
        this.bootstaps = bootstaps;
    }

    public void connect() {
        bootstaps.forEach(hostConnector::bootstrap);
        hostConnector.connect();
    }

    @Override
    public void onConnectionChanged(boolean isConnected, final NetworkContext nc) {

        if (!isConnected)
            onClose();

        connectionManager.onConnectionChanged(isConnected, nc);
    }

    private void onClose() {

        if (terminated.get()) {
            closeQuietly(hostConnector);
            return;
        }

        hostConnector.reconnect();
    }

    @Override
    public void onTerminate(final NetworkContext nc) {
        terminated.set(true);
        hostConnector.close();
        connectionManager.onConnectionChanged(false, nc);
    }

    @Override
    public boolean isTerminated() {
        return terminated.get();
    }
}
