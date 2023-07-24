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

import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;

public class HostDetails extends SelfDescribingMarshallable {

    private int hostId;
    private int tcpBufferSize;
    private String connectUri;
    private ConnectionStrategy connectionStrategy;
    private String[] remoteConnectUris;

    public int tcpBufferSize() {
        return tcpBufferSize;
    }

    @NotNull
    public HostDetails hostId(int hostId) {
        this.hostId = hostId;
        return this;
    }

    /**
     * This is the URI of the socked we bind to, to accept connections
     *
     * @return The socket URI (e.g. "192.168.1.35:9876", or "0.0.0.0:2345" to bind to all addresses)
     */
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

    /**
     * A list of URIs remote nodes should use to connect to this host
     * <p>
     * Leave as null if {@link #connectUri()} is the only address used to establish connections
     *
     * @return The list of URIs to round-robin over to connect to this host, or null to use {@link #connectUri()}
     */
    public String[] remoteConnectUris() {
        return remoteConnectUris;
    }

    public HostDetails remoteConnectUris(String[] socketConnectHostPort) {
        this.remoteConnectUris = socketConnectHostPort;
        return this;
    }

    /**
     * The connection strategy to use to connect to this host
     *
     * @return The connection strategy, or null to use the default
     */
    public ConnectionStrategy connectionStrategy() {
        return connectionStrategy;
    }

    public HostDetails connectionStrategy(ConnectionStrategy connectionStrategy) {
        this.connectionStrategy = connectionStrategy;
        return this;
    }
}
