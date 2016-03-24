package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Rob Austin.
 */
abstract public class Cluster<E extends HostDetails, C extends ClusterContext> implements Marshallable,
        Closeable {

    private final Map<String, E> hostDetails;
    private final String clusterName;

    private C clusterContext;

    public Cluster(String clusterName) {
        hostDetails = new ConcurrentSkipListMap<>();
        this.clusterName = clusterName;
    }

    protected C clusterContext() {
        return (C) clusterContext;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {

        hostDetails.clear();
        StringBuilder hostDescription = new StringBuilder();

        if (!wire.hasMore())
            return;


        ValueIn valueIn = wire.readEventName(hostDescription);

        if ("context".contentEquals(hostDescription)) {
            clusterContext = (C) valueIn.typedMarshallable();
            clusterContext.clusterName(clusterName);
            if (!wire.hasMore())
                return;

            valueIn = wire.readEventName(hostDescription);
        }

        for (; ; ) {

            valueIn.marshallable(details -> {
                final E hd = newHostDetails();
                hd.readMarshallable(details);
                hostDetails.put(hostDescription.toString(), hd);
            });

            if (!wire.hasMore())
                break;

            valueIn = wire.readEventName(hostDescription);
        }
    }

    @Nullable
    HostDetails findHostDetails(int remoteIdentifier) {

        for (HostDetails hd : hostDetails.values()) {
            if (hd.hostId() == remoteIdentifier)
                return hd;
        }
        return null;
    }


    public <H extends HostDetails, C extends ClusterContext> ConnectionStrategy
    findConnectionStrategy(int remoteIdentifier) {

        HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionStrategy();
    }


    public ConnectionManager findConnectionManager(int remoteIdentifier) {
        HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.connectionEventManagerHandler();
    }


    public TerminationEventHandler findTerminationEventHandler(int remoteIdentifier) {
        HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.terminationEventHandler();

    }
/*

    public HostConnector findHostConnector(int remoteIdentifier) {
        HostDetails hostDetails = findHostDetails(remoteIdentifier);
        if (hostDetails == null) return null;
        return hostDetails.hostConnector();
    }
*/


    abstract protected E newHostDetails();

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (Map.Entry<String, E> entry2 : hostDetails.entrySet()) {
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
