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
package net.openhft.chronicle.network.api;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;

import java.util.UUID;

public class NetworkStats extends SelfDescribingMarshallable {

    private long writeBps;
    private long readBps;
    private long timestamp;
    private long index;

    private long writeEwma;
    private long readEwma;
    private long socketPollRate;

    private int localIdentifier;
    private int p50;
    private int p90;
    private int p99;
    private int p99_9;
    private int remoteIdentifier;

    private boolean connected;
    private boolean acceptor;

    private WireType wireType;
    private UUID clientId;
    private String remoteHostName;
    private int remotePort;
    private String userId;
    private String host;

    public String userId() {
        return userId;
    }

    public NetworkStats userId(String userId) {
        this.userId = userId;
        return this;
    }

    /**
     * Bytes written per second
     *
     * @return write Bytes/sec
     */
    public long writeBps() {
        return writeBps;
    }

    public NetworkStats writeBps(long writeBps) {
        this.writeBps = writeBps;
        return this;
    }

    /**
     * Bytes read per second
     *
     * @return read Bytes/sec
     */
    public long readBps() {
        return readBps;
    }

    public NetworkStats readBps(long readBps) {
        this.readBps = readBps;
        return this;
    }

    public long writeEwma() {
        return writeEwma;
    }

    public NetworkStats writeEwma(long writeEwma) {
        this.writeEwma = writeEwma;
        return this;
    }

    public long readEwma() {
        return readEwma;
    }

    public NetworkStats readEwma(long readEwma) {
        this.readEwma = readEwma;
        return this;
    }

    public long index() {
        return index;
    }

    public NetworkStats index(long index) {
        this.index = index;
        return this;
    }

    /**
     * Timestamp of last update to this, from {@link System#currentTimeMillis()}
     *
     * @return timestamp in millis
     */
    public long timestamp() {
        return timestamp;
    }

    public NetworkStats timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Socket polls per second
     *
     * @return socket polls/sec
     */
    public long socketPollRate() {
        return socketPollRate;
    }

    public NetworkStats socketPollRate(long socketPollRate) {
        this.socketPollRate = socketPollRate;
        return this;
    }

    /**
     * Local identifier (hostId)
     *
     * @return local hostId
     */
    public int localIdentifier() {
        return localIdentifier;
    }

    public NetworkStats localIdentifier(int localIdentifier) {
        this.localIdentifier = localIdentifier;
        return this;
    }

    public int p50() {
        return p50;
    }

    public NetworkStats p50(int p50) {
        this.p50 = p50;
        return this;
    }

    public int p90() {
        return p90;
    }

    public NetworkStats p90(int p90) {
        this.p90 = p90;
        return this;
    }

    public int p99() {
        return p99;
    }

    public NetworkStats p99(int p99) {
        this.p99 = p99;
        return this;
    }

    public int p99_9() {
        return p99_9;
    }

    public NetworkStats p99_9(int p99_9) {
        this.p99_9 = p99_9;
        return this;
    }

    public int remoteIdentifier() {
        return remoteIdentifier;
    }

    public NetworkStats remoteIdentifier(int remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
        return this;
    }

    public boolean connected() {
        return connected;
    }

    public NetworkStats connected(boolean connected) {
        this.connected = connected;
        return this;
    }

    public boolean acceptor() {
        return acceptor;
    }

    public NetworkStats acceptor(boolean acceptor) {
        this.acceptor = acceptor;
        return this;
    }

    public WireType wireType() {
        return wireType;
    }

    public NetworkStats wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public UUID clientId() {
        return clientId;
    }

    public NetworkStats clientId(UUID clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Remote host name as determined at connection time
     *
     * @return remote host name
     */
    public String remoteHostName() {
        return remoteHostName;
    }

    public NetworkStats remoteHostName(String remoteHostName) {
        this.remoteHostName = remoteHostName;
        return this;
    }

    /**
     * Remote port as determined at connection time
     *
     * @return remote port
     */
    public int remotePort() {
        return remotePort;
    }

    public NetworkStats remotePort(int remotePort) {
        this.remotePort = remotePort;
        return this;
    }

    public String host() {
        return host;
    }

    public NetworkStats host(String host) {
        this.host = host;
        return this;
    }
}