package net.openhft.chronicle.network.cluster;

/**
 * @author Rob Austin.
 */

import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * Handles the connection strategy ( in other words when to accept or reject a connection ) and
 * heart beating )
 *
 * @author Rob Austin.
 */
public class HostIdConnectionStrategy implements ConnectionStrategy, Demarshallable, WriteMarshallable {

    private HostIdConnectionStrategy(WireIn w) {
    }

    HostIdConnectionStrategy() {
    }

    /**
     * @return false if already connected
     */
    public synchronized boolean notifyConnected(@NotNull WireTcpHandler handler,
                                                int localIdentifier,
                                                int remoteIdentifier) {

        return !(handler.nc().isAcceptor() && localIdentifier < remoteIdentifier);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

    }


}

