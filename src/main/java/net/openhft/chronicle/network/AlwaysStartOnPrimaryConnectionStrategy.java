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
package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.connection.*;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.connection.ConnectionState.ESTABLISHED;

/**
 * Loops through all the hosts:ports ( in order ) starting at the primary, till it finds a host that it can connect to.
 * If later, this successful connection is dropped, it will always return to the primary to begin attempting to find a successful connection,
 * If all the host:ports have been attempted since the last connection was established, no successful connection can be found,
 * then null is returned, and the fatalFailureMonitor.onFatalFailure() is triggered
 */
public class AlwaysStartOnPrimaryConnectionStrategy extends AbstractConnectionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AlwaysStartOnPrimaryConnectionStrategy.class);

    private int pausePeriodMs = Jvm.getInteger("client.timeout", 500);
    private int socketConnectionTimeoutMs = Jvm.getInteger("connectionStrategy.socketConnectionTimeoutMs", 1);
    private long pauseMillisBeforeReconnect = Jvm.getInteger("connectionStrategy.pauseMillisBeforeReconnect", 500);
    private long minPauseSec = defaultMinPauseSec();
    private long maxPauseSec = defaultMaxPauseSec();

    public AlwaysStartOnPrimaryConnectionStrategy clientConnectionMonitor(ClientConnectionMonitor fatalFailureMonitor) {
        this.clientConnectionMonitor = fatalFailureMonitor;
        return this;
    }

    @Override
    public ClientConnectionMonitor clientConnectionMonitor() {
        return clientConnectionMonitor;
    }

    @Nullable
    @Override
    public ChronicleSocketChannel connect(@NotNull String name,
                                          @NotNull SocketAddressSupplier socketAddressSupplier,
                                          @NotNull ConnectionState previousConnectionState,
                                          @Nullable FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {

        if (socketAddressSupplier.get() == null || previousConnectionState == ESTABLISHED)
            socketAddressSupplier.resetToPrimary();
        else
            socketAddressSupplier.failoverToNextAddress();

        if (fatalFailureMonitor == null)
            fatalFailureMonitor = FatalFailureMonitor.NO_OP;

        for (; ; ) {
            throwExceptionIfClosed();
            ChronicleSocketChannel socketChannel = null;
            try {

                @Nullable final InetSocketAddress socketAddress = socketAddressSupplier.get();
                if (socketAddress == null) {
                    Jvm.warn().on(AlwaysStartOnPrimaryConnectionStrategy.class, "failed to obtain socketAddress");
                    // at end
                    if (isAtEnd(socketAddressSupplier)) {
                        fatalFailureMonitor.onFatalFailure(name, "Failed to connect to any of these servers=" + socketAddressSupplier.remoteAddresses());
                        return null;
                    }
                    socketAddressSupplier.failoverToNextAddress();
                    continue;
                }

                socketChannel = openSocketChannel(socketAddress, tcpBufferSize, pausePeriodMs, socketConnectionTimeoutMs);

                if (socketChannel == null) {
                    if (Jvm.isDebugEnabled(getClass()))
                        Jvm.debug().on(getClass(), "unable to connected to " + socketAddressSupplier);

                    // at end
                    if (isAtEnd(socketAddressSupplier)) {
                        fatalFailureMonitor.onFatalFailure(name, "Failed to connect to any of these servers=" + socketAddressSupplier.remoteAddresses());
                        return null;
                    }

                    socketAddressSupplier.failoverToNextAddress();
                    continue;
                }

                Jvm.debug().on(getClass(), "successfully connected to " + socketAddressSupplier);

                // success
                return socketChannel;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Throwable e) {
                //noinspection ConstantConditions
                if (socketChannel != null)
                    closeQuietly(socketChannel);

                if (Jvm.isDebug())
                    LOG.info("", e);

                socketAddressSupplier.failoverToNextAddress();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(pausePeriodMs));
            }
        }
    }

    private boolean isAtEnd(SocketAddressSupplier socketAddressSupplier) {
        return socketAddressSupplier.size() - 1 == socketAddressSupplier.index();
    }

    @Override
    public long pauseMillisBeforeReconnect() {
        return pauseMillisBeforeReconnect;
    }

    public AlwaysStartOnPrimaryConnectionStrategy tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    public AlwaysStartOnPrimaryConnectionStrategy pausePeriodMs(int pausePeriodMs) {
        this.pausePeriodMs = pausePeriodMs;
        return this;
    }

    public AlwaysStartOnPrimaryConnectionStrategy socketConnectionTimeoutMs(int socketConnectionTimeoutMs) {
        this.socketConnectionTimeoutMs = socketConnectionTimeoutMs;
        return this;
    }

    public AlwaysStartOnPrimaryConnectionStrategy pauseMillisBeforeReconnect(long pauseMillisBeforeReconnect) {
        this.pauseMillisBeforeReconnect = pauseMillisBeforeReconnect;
        return this;
    }

    @Override
    public long minPauseSec() {
        return minPauseSec;
    }

    @Override
    public long maxPauseSec() {
        return maxPauseSec;
    }

    public AlwaysStartOnPrimaryConnectionStrategy minPauseSec(long minPauseSec) {
        this.minPauseSec = minPauseSec;
        return this;
    }

    public AlwaysStartOnPrimaryConnectionStrategy maxPauseSec(long maxPauseSec) {
        this.maxPauseSec = maxPauseSec;
        return this;
    }
}
