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

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

public class HostDetails implements Marshallable {

    ClusterNotifier clusterNotifier;
    private int hostId;
    private int tcpBufferSize;
    private String connectUri;
    private int timeoutMs;
    private ConnectionStrategy connectionStrategy;
    private ConnectionManager connectionManager;
    private TerminationEventHandler terminationEventHandler;
    private HostConnector hostConnector;

    public HostDetails() {
    }

    public int tcpBufferSize() {
        return tcpBufferSize;
    }

    @NotNull
    public HostDetails hostId(int hostId) {
        this.hostId = hostId;
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "hostId").int32(this, (o, i) -> o.hostId = i)
                .read(() -> "tcpBufferSize").int32(this, (o, i) -> o.tcpBufferSize = i)
                .read(() -> "connectUri").text(this, (o, i) -> o.connectUri = i)
                .read(() -> "timeoutMs").int32(this, (o, i) -> o.timeoutMs = i);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "hostId").int32(hostId)
                .write(() -> "tcpBufferSize").int32(tcpBufferSize)
                .write(() -> "connectUri").text(connectUri);
    }

    public String connectUri() {
        return connectUri;
    }

    public int hostId() {
        return hostId;
    }

    @NotNull
    public HostDetails tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    @NotNull
    public HostDetails connectUri(@NotNull String connectUri) {
        this.connectUri = connectUri;
        return this;
    }

    @NotNull
    public HostDetails timeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public ConnectionStrategy connectionStrategy() {
        return connectionStrategy;
    }

    public ConnectionManager connectionManager() {
        return connectionManager;
    }

    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    public void connectionStrategy(@NotNull ConnectionStrategy connectionStrategy) {
        this.connectionStrategy = connectionStrategy;
    }

    public void connectionManager(@NotNull ConnectionManager connectionEventManagerHandler) {
        this.connectionManager = connectionEventManagerHandler;
    }

    public void terminationEventHandler(@NotNull TerminationEventHandler terminationEventHandler) {
        this.terminationEventHandler = terminationEventHandler;
    }

    public void hostConnector(HostConnector hostConnector) {
        this.hostConnector = hostConnector;
    }

    public HostConnector hostConnector() {
        return hostConnector;
    }

    public ClusterNotifier clusterNotifier() {
        return clusterNotifier;
    }

    public void clusterNotifier(ClusterNotifier clusterHandler) {
        this.clusterNotifier = clusterHandler;
    }
}
