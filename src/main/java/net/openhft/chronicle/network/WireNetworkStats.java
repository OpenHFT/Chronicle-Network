/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class WireNetworkStats extends AbstractMarshallable implements
        NetworkStats<WireNetworkStats> {
    private long writeBps, readBps, socketPollCountPerSecond;
    private long timestamp;
    private int localIdentifier;
    private int remoteIdentifier;
    private String remoteHostName;
    private int remotePort;
    private String userId;
    private UUID clientId;
    private boolean isConnected;
    private Enum wireType;
    private boolean isAcceptor;
    private long percentile90th;
    private long percentile50th;
    private long percentile99th;

    private long percentile99_9th;

    public WireNetworkStats(int localIdentifier) {
        this.localIdentifier = localIdentifier;
    }

    public WireNetworkStats() {
    }

    public long percentile90th() {
        return percentile90th;
    }

    public long percentile50th() {
        return percentile50th;
    }

    public long percentile99th() {
        return percentile99th;
    }

    public long percentile99_9th() {
        return percentile99_9th;
    }

    public Enum wireType() {
        return wireType;
    }

    @NotNull
    public WireNetworkStats wireType(Enum wireType) {
        this.wireType = wireType;
        return this;
    }

    @Override
    public String userId() {
        return userId;
    }

    @NotNull
    @Override
    public WireNetworkStats userId(String userId) {
        this.userId = userId;
        return this;
    }

    @Override
    public long writeBps() {
        return writeBps;
    }

    @NotNull
    @Override
    public WireNetworkStats writeBps(long writeBps) {
        this.writeBps = writeBps;
        return this;
    }

    @Override
    public long readBps() {
        return readBps;
    }

    @NotNull
    @Override
    public WireNetworkStats readBps(long readBps) {
        this.readBps = readBps;
        return this;
    }

    @Override
    public long socketPollCountPerSecond() {
        return socketPollCountPerSecond;
    }

    @NotNull
    @Override
    public WireNetworkStats socketPollCountPerSecond(long socketPollCountPerSecond) {
        this.socketPollCountPerSecond = socketPollCountPerSecond;
        return this;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @NotNull
    @Override
    public WireNetworkStats timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @NotNull
    @Override
    public synchronized WireNetworkStats remoteHostName(@NotNull String hostName) {
        this.remoteHostName = hostName;
        return this;
    }

    @Override
    public synchronized void remotePort(int port) {
        this.remotePort = port;
    }

    @Override
    public int localIdentifier() {
        return localIdentifier;
    }

    @NotNull
    @Override
    public WireNetworkStats localIdentifier(int localIdentifier) {
        this.localIdentifier = localIdentifier;
        return this;
    }

    @Override
    public boolean isAcceptor() {
        return this.isAcceptor;
    }

    @Override
    public void isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
    }

    @Override
    public int remoteIdentifier() {
        return remoteIdentifier;
    }

    @NotNull
    @Override
    public WireNetworkStats remoteIdentifier(int remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
        return this;
    }

    @Override
    public void clientId(UUID clientId) {
        this.clientId = clientId;
    }

    @Override
    public UUID clientId() {
        return clientId;
    }

    @Override
    public synchronized String remoteHostName() {
        return remoteHostName;
    }

    @Override
    public synchronized int remotePort() {
        return remotePort;
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public void isConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }

    @Override
    public void percentile50th(long percentile50th) {
        this.percentile50th = percentile50th;
    }

    @Override
    public void percentile90th(long percentile90th) {
        this.percentile90th = percentile90th;
    }

    @Override
    public void percentile99th(long percentile99th) {
        this.percentile99th = percentile99th;
    }

    @Override
    public void percentile99_9th(long percentile99_9th) {
        this.percentile99_9th = percentile99_9th;
    }

}