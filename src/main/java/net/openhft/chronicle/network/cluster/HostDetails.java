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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

public class HostDetails implements Marshallable {


    private int hostId;
    private int tcpBufferSize;
    private String connectUri;
    private int timeoutMs;

    public int tcpBufferSize() {
        return tcpBufferSize;
    }

    private ConnectionStrategy connectionStrategy;
    private ConnectionManager connectionEventManagerHandler;
    private TerminationEventHandler terminationEventHandler;
    private HostConnector hostConnector;

    public HostDetails() {
    }

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
                .write(() -> "connectUri").text(connectUri)
                .write(() -> "timeoutMs").int32(timeoutMs);
    }


    public String connectUri() {
        return connectUri;
    }

    public long timeoutMs() {
        return timeoutMs();
    }

    public int hostId() {
        return hostId;
    }


    public HostDetails tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    public HostDetails connectUri(String connectUri) {
        this.connectUri = connectUri;
        return this;
    }

    public HostDetails timeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }


    public ConnectionStrategy connectionStrategy() {
        return connectionStrategy;
    }

    public ConnectionManager connectionEventManagerHandler() {
        return connectionEventManagerHandler;
    }

    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    public void connectionStrategy(ConnectionStrategy connectionStrategy) {
        this.connectionStrategy = connectionStrategy;
    }

    public void connectionEventManagerHandler(ConnectionManager connectionEventManagerHandler) {
        this.connectionEventManagerHandler = connectionEventManagerHandler;
    }

    public void terminationEventHandler(TerminationEventHandler terminationEventHandler) {
        this.terminationEventHandler = terminationEventHandler;
    }

    public void hostConnector(HostConnector hostConnector) {
        this.hostConnector = hostConnector;
    }

    public HostConnector hostConnector() {
        return hostConnector;
    }

    ClusterNotifier clusterHandler;

    public ClusterNotifier clusterHandler() {
        return clusterHandler;
    }

    public void clusterHandler(ClusterNotifier clusterHandler) {
        this.clusterHandler = clusterHandler;
    }
}
