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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Rob Austin.
 */
abstract public class Cluster<E extends HostDetails, C extends ClusterContext> implements Marshallable,
        Closeable {

    @NotNull
    private final Map<String, E> hostDetails;
    private final String clusterName;

    @org.jetbrains.annotations.Nullable
    private C clusterContext;

    public Cluster(String clusterName) {
        hostDetails = new ConcurrentSkipListMap<>();
        this.clusterName = clusterName;
    }

    public String clusterName() {
        return clusterName;
    }

    @org.jetbrains.annotations.Nullable
    public C clusterContext() {
        return (C) clusterContext;
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
                clusterContext = (C) valueIn.typedMarshallable();
                clusterContext.clusterName(clusterName);
                continue;
            }

            valueIn.marshallable(details -> {
                @NotNull final E hd = newHostDetails();
                hd.readMarshallable(details);
                hostDetails.put(sb.toString(), hd);
            });

        }

        // commented out as this causes issues with the chronicle-engine gui
        //  if (clusterContext == null)
        //       throw new IllegalStateException("required field 'context' is missing.");

    }

    @org.jetbrains.annotations.Nullable
    @Nullable
    public HostDetails findHostDetails(int id) {

        for (@NotNull HostDetails hd : hostDetails.values()) {
            if (hd.hostId() == id)
                return hd;
        }
        return null;
    }

    @org.jetbrains.annotations.Nullable
    public <H extends HostDetails, C extends ClusterContext> ConnectionStrategy
    findConnectionStrategy(int remoteIdentifier) {

        @org.jetbrains.annotations.Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionStrategy();
    }

    @org.jetbrains.annotations.Nullable
    public ConnectionManager findConnectionManager(int remoteIdentifier) {
        @org.jetbrains.annotations.Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionManager();
    }

    @org.jetbrains.annotations.Nullable
    public TerminationEventHandler findTerminationEventHandler(int remoteIdentifier) {
        @org.jetbrains.annotations.Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.terminationEventHandler();

    }

    @org.jetbrains.annotations.Nullable
    public ConnectionChangedNotifier findClusterNotifier(int remoteIdentifier) {
        @org.jetbrains.annotations.Nullable HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.clusterNotifier();
    }

    @NotNull
    abstract protected E newHostDetails();

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (@NotNull Map.Entry<String, E> entry2 : hostDetails.entrySet()) {
            wire.writeEventName(entry2::getKey).marshallable(entry2.getValue());
        }
    }

    @NotNull
    public Collection<E> hostDetails() {
        return hostDetails.values();
    }

    @Override
    public void close() {
        hostDetails().forEach(Closeable::closeQuietly);
    }

    public void install() {
        if (clusterContext != null && hostDetails != null && hostDetails.values() != null)
            hostDetails.values().forEach(clusterContext::accept);
    }
}
