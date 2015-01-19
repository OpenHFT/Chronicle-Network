/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.internal;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class NetworkConfig implements Serializable {

    private static final int DEFAULT_TCP_BUFFER_SIZE = 1024 * 64;
    private static final long DEFAULT_HEART_BEAT_INTERVAL = 20;
    private static final TimeUnit DEFAULT_HEART_BEAT_INTERVAL_UNIT = SECONDS;

    private int tcpBufferSize = DEFAULT_TCP_BUFFER_SIZE;
    private boolean autoReconnectedUponDroppedConnection;
    private ThrottlingConfig throttlingConfig = ThrottlingConfig.noThrottling();
    private long heartBeatInterval;
    private TimeUnit heartBeatIntervalUnit = TimeUnit.MILLISECONDS;
    private String name = "(unknown)";
    private Set<InetSocketAddress> endpoints = new HashSet<InetSocketAddress>();

    private InetSocketAddress inetSocketAddress;
    ;


    private NetworkConfig(InetSocketAddress inetSocketAddress) {
        this.inetSocketAddress = inetSocketAddress;
    }

    public static int getDefaultTcpBufferSize() {
        return DEFAULT_TCP_BUFFER_SIZE;
    }

    public static long getDefaultHeartBeatInterval() {
        return DEFAULT_HEART_BEAT_INTERVAL;
    }

    public static TimeUnit getDefaultHeartBeatIntervalUnit() {
        return DEFAULT_HEART_BEAT_INTERVAL_UNIT;
    }

    public int getPort() {
        return this.inetSocketAddress.getPort();
    }

    public static NetworkConfig port(int port) {
        return new NetworkConfig(new InetSocketAddress(port));
    }

    public int tcpBufferSize() {
        return tcpBufferSize;
    }

    public NetworkConfig tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    public boolean isAutoReconnectedUponDroppedConnection() {
        return autoReconnectedUponDroppedConnection;
    }

    public NetworkConfig autoReconnectedUponDroppedConnection(boolean
                                                                      autoReconnectedUponDroppedConnection) {
        this.autoReconnectedUponDroppedConnection = autoReconnectedUponDroppedConnection;
        return this;
    }

    public ThrottlingConfig throttlingConfig() {
        return throttlingConfig;
    }

    public NetworkConfig throttlingConfig(ThrottlingConfig throttlingConfig) {
        this.throttlingConfig = throttlingConfig;
        return this;
    }

    public long heartBeatInterval() {
        return heartBeatInterval;
    }

    public NetworkConfig heartBeatInterval(long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }

    public TimeUnit heartBeatIntervalUnit() {
        return heartBeatIntervalUnit;
    }

    public NetworkConfig setHeartBeatIntervalUnit(TimeUnit heartBeatIntervalUnit) {
        this.heartBeatIntervalUnit = heartBeatIntervalUnit;
        return this;
    }


    public NetworkConfig name(String name) {
        this.name = name;
        return this;
    }

    public Set<InetSocketAddress> endpoints() {
        return endpoints;
    }

    public NetworkConfig setEndpoints(InetSocketAddress... endpoints) {
        this.endpoints = new HashSet<InetSocketAddress>(asList(endpoints));
        return this;
    }

    public InetSocketAddress inetSocketAddress() {
        return inetSocketAddress;
    }

    public boolean autoReconnectedUponDroppedConnection() {
        return true;
    }

    public int serverPort() {
        return inetSocketAddress.getPort();
    }


    public long heartBeatInterval(TimeUnit unit) {
        return unit.convert(heartBeatInterval, heartBeatIntervalUnit);
    }


    public String name() {
        return name;
    }
}
