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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

abstract public class Cluster<E extends HostDetails, T extends ClusteredNetworkContext<T>, C extends ClusterContext<T>> extends AbstractCloseable implements Marshallable {

    @NotNull
    public final Map<String, E> hostDetails;
    private final String clusterName;

    private C context;

    public Cluster(String clusterName) {
        hostDetails = new ConcurrentSkipListMap<>();
        this.clusterName = clusterName;
    }

    public String clusterName() {
        return clusterName;
    }

    public C clusterContext() {
        return context;
    }

    public void clusterContext(@NotNull C clusterContext) {
        throwExceptionIfClosed();
        this.context = clusterContext;
        clusterContext.clusterName(clusterName);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        hostDetails.clear();

        if (wire.isEmpty())
            return;
        while (!wire.isEmpty()) {
            final StringBuilder sb = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = wire.readEventName(sb);

            if ("context".contentEquals(sb)) {
                context = (C) valueIn.object(ClusterContext.class);
                assert context != null;
                context.clusterName(clusterName);
                continue;
            }

            valueIn.marshallable(details -> {
                @NotNull final E hd = newHostDetails();
                hd.readMarshallable(details);
                hostDetails.put(sb.toString(), hd);
            });

        }

        // commented out as this causes issues with the chronicle-engine gui
        //  if (context == null)
        //       throw new IllegalStateException("required field 'context' is missing.");
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("context").typedMarshallable(context);

        for (@NotNull Map.Entry<String, E> entry2 : hostDetails.entrySet()) {
            wire.writeEventName(entry2::getKey).marshallable(entry2.getValue());
        }
    }

    @Nullable
    public E findHostDetails(int id) {
        throwExceptionIfClosed();

        for (@NotNull E hd : hostDetails.values()) {
            if (hd.hostId() == id)
                return hd;
        }
        return null;
    }

    @Nullable
    public ConnectionNotifier findConnectionNotifier(int remoteIdentifier) {
        throwExceptionIfClosed();

        @Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionNotifier();
    }

    @Nullable
    public ConnectionManager<T> findConnectionManager(int remoteIdentifier) {
        throwExceptionIfClosed();
        @Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionManager();
    }

    @Nullable
    public TerminationEventHandler<T> findTerminationEventHandler(int remoteIdentifier) {
        throwExceptionIfClosed();
        @Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.terminationEventHandler();

    }

    @Nullable
    public ConnectionChangedNotifier<T> findClusterNotifier(int remoteIdentifier) {
        throwExceptionIfClosed();
        @Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.clusterNotifier();
    }

    @NotNull
    abstract protected E newHostDetails();

    @NotNull
    public Collection<E> hostDetails() {
        return hostDetails.values();
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(hostDetails());
    }

    public void install() {
        Set<Integer> hostIds = hostDetails.values().stream().map(HostDetails::hostId).collect(Collectors.toSet());

        int local = context.localIdentifier();
        if (!hostIds.contains(local)) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "cluster='" + context.clusterName() + "' ignored as localIdentifier=" + context.localIdentifier() + " is in this cluster");
            return;
        }

        if (context != null)
            hostDetails.values().forEach(context);
    }

    @Override
    protected boolean threadSafetyCheck() {
        // assume thread safe
        return true;
    }
}
