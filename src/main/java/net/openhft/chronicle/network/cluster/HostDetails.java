/*
 * Copyright 2016-2020 Chronicle Software
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

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;

public class HostDetails extends SelfDescribingMarshallable {

    private int hostId;
    private int tcpBufferSize;
    private String connectUri;
    private String region;
    private int timeoutMs;
    private transient ClusterNotifier clusterNotifier;
    private transient ConnectionNotifier connectionNotifier;
    private transient ConnectionManager connectionManager;
    private transient TerminationEventHandler terminationEventHandler;
    private transient HostConnector hostConnector;

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

    public String region() {
        return region;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    public ConnectionNotifier connectionNotifier() {
        return connectionNotifier;
    }

    public ConnectionManager connectionManager() {
        return connectionManager;
    }

    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    public void connectionNotifier(ConnectionNotifier connectionStrategy) {
        this.connectionNotifier = connectionStrategy;
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
