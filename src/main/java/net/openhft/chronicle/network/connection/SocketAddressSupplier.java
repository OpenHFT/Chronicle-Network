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

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.TCPRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Provides support for the client to failover TCP connections to different servers, if the primary
 * connection can not be establish, after retrying up to a timeout,  see {@link
 * SocketAddressSupplier#timeoutMS()} the other connections will be attempted. The order of these
 * connections are determined by the order of the connectURIs
 *
 * @author Rob Austin.
 */
public class SocketAddressSupplier implements Supplier<SocketAddress> {

    private static final Logger LOG = LoggerFactory.getLogger(SocketAddressSupplier.class);
    @NotNull
    private final String name;
    private final List<RemoteAddressSupplier> remoteAddresses = new ArrayList<>();
    private final long failoverTimeout = Integer.getInteger("tcp.failover.time", 2_000);
    @Nullable
    private RemoteAddressSupplier current;
    private int addressCount = 0;

    /**
     * @param connectURIs the socket connections defined in order with the primary first
     * @param name        the name of this service
     */
    public SocketAddressSupplier(@NotNull final String[] connectURIs, @NotNull final String name) {
        this.name = name;
        for (@NotNull String connectURI : connectURIs) {
            this.remoteAddresses.add(new RemoteAddressSupplier(connectURI));
        }

        assert !this.remoteAddresses.isEmpty();
    }

    /**
     * use this method if you only with to connect to a single server
     *
     * @param connectURI the uri of the server
     * @return a SocketAddressSupplier containing the UIR you provide
     */
    @NotNull
    public static SocketAddressSupplier uri(String connectURI) {
        return new SocketAddressSupplier(new String[]{connectURI}, "");
    }

    @NotNull
    public List<RemoteAddressSupplier> all() {
        return remoteAddresses;
    }

    @NotNull
    public String name() {
        return name;
    }

    public void failoverToNextAddress() {
        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "failing over to next address");
        next();
    }

    /**
     * reset back to the primary
     */
    public void resetToPrimary() {
        addressCount = 0;
        current = remoteAddresses.get(addressCount);
    }

    private void next() {
        addressCount = (addressCount + 1) % remoteAddresses.size();
        current = remoteAddresses.get(addressCount);
    }

    public int size() {
        return remoteAddresses.size();
    }

    /**
     * @return index ( primary has index of ZERO )
     */
    public int index() {
        return addressCount;
    }

    public long timeoutMS() {
        return failoverTimeout;
    }

    @Nullable
    @Override
    public InetSocketAddress get() {
        @Nullable final RemoteAddressSupplier current = this.current;
        if (current == null)
            return null;
        return current.get();
    }

    @Nullable
    public String getDescription() {
        @Nullable final RemoteAddressSupplier current = this.current;
        if (current == null)
            return null;
        return current.toString();
    }

    @Override
    @NotNull
    public String toString() {
        return log(this.current);
    }

    public String remoteAddresses() {
        List<String> result = new ArrayList<>();
        remoteAddresses.forEach(r -> result.add(log(r)));
        return result.toString();

    }

    @NotNull
    public String log(RemoteAddressSupplier current) {
        if (current == null)
            return "(none)";

        final SocketAddress socketAddress = current.get();
        if (socketAddress == null)
            return "(none)";

        return socketAddress.toString().replaceAll("0:0:0:0:0:0:0:0", "localhost") + " - " +
                current.toString();
    }

    private class RemoteAddressSupplier implements Supplier<SocketAddress> {

        private final InetSocketAddress remoteAddress;
        @NotNull
        private final String description;

        public RemoteAddressSupplier(@NotNull String description) {
            this.description = description;
            remoteAddress = TCPRegistry.lookup(description);
        }

        @Override
        public InetSocketAddress get() {
            return remoteAddress;
        }

        @NotNull
        @Override
        public String toString() {
            return description;
        }
    }
}
