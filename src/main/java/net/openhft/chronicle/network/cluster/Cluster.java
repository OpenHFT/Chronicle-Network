/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

abstract public class Cluster<T extends ClusteredNetworkContext<T>, C extends ClusterContext<C, T>>
        extends SimpleCloseable
        implements Marshallable {

    @NotNull
    public final Map<String, HostDetails> hostDetails;

    private C context;

    public Cluster() {
        hostDetails = new ConcurrentSkipListMap<>();
    }

    public C clusterContext() {
        return context;
    }

    public void clusterContext(@NotNull C clusterContext) {
        throwExceptionIfClosed();

        this.context = clusterContext;
    }

    @Override
    // synchronized guarding hostDetails
    public synchronized void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        hostDetails.clear();

        if (wire.isEmpty())
            return;
        while (!wire.isEmpty()) {
            final StringBuilder sb = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = wire.readEventName(sb);

            if ("context".contentEquals(sb)) {
                context = (C) valueIn.object(ClusterContext.class);
                assert context != null;
                continue;
            }

            valueIn.marshallable(details -> {
                @NotNull final HostDetails hd = new HostDetails();
                hd.readMarshallable(details);
                hostDetails.put(sb.toString(), hd);
            });
        }

    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("context").typedMarshallable(context);

        for (@NotNull Map.Entry<String, HostDetails> entry2 : hostDetails.entrySet()) {
            wire.writeEventName(entry2::getKey).marshallable(entry2.getValue());
        }
    }

    @Nullable
    public HostDetails findHostDetails(int id) {
        throwExceptionIfClosed();

        for (@NotNull HostDetails hd : hostDetails.values()) {
            if (hd.hostId() == id)
                return hd;
        }
        return null;
    }

    @NotNull
    public Collection<HostDetails> hostDetails() {
        throwExceptionIfClosed();
        return hostDetails.values();
    }

    @Override
    // synchronized guarding hostDetails
    protected synchronized void performClose() {
        Closeable.closeQuietly(context, hostDetails);
        hostDetails.clear();
    }

    // synchronized guarding hostDetails
    public synchronized void start(int localHostId) {
        final Optional<HostDetails> acceptOn = hostDetails.values().stream().filter(hd -> hd.hostId() == localHostId).findAny();

        if (!acceptOn.isPresent())
            throw new IllegalArgumentException("Cannot start cluster member as provided hostid=" + localHostId + " is not found in the cluster");

        if (context == null)
            throw new IllegalStateException("Cannot start cluster member as the cluster context is null");

        context.localIdentifier((byte) localHostId);

        hostDetails.values().stream().filter(hd -> hd.hostId() != localHostId).forEach(context::connect);
        context.eventLoop().start();
        context.accept(acceptOn.get());
    }
}
