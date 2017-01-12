package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;

import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 *         The main functionality would be exactly same as C#:
 *         -       on any disconnect event, Chronicle Client would go through the list of hosts to connect for a pre-configured number of times (default connection retries with each host = 3).
 *         -       A fatal failure event is raised if the connection is not established during any of these attempts.
 *         -       If connection is successful with any hosts in the list, then fatal failure is not raised, and connection retries is set to zero (so that any future disconnection event then starts its own “3 retries for each host”).
 *         <p>
 *         Couple of examples are given below. The assumption is that there are three servers MAIN, DR1 and DR2. The
 *         1.     Currently the client is connected with DR1. A disconnect with DR1 occurs, and then the client tries connecting to DR2, MAIN and DR1. It attempts this for a preconfigured number of times (e.g. 3 times).  So for example, the following events occur:
 *         --a.     Connection attempt no 1 with  DR2:  failed
 *         --b.    Connection attempt no 1 with MAIN:  failed
 *         --c.     Connection attempt no 1 with DR1:  failed
 *         --d.    Connection attempt no 2 with DR2:  succeeded. No fatal failure event is raised.
 *         2.     Now the client is on DR2 and a disconnect with DR2 occurs. Following events occur
 *         --a.     Connection attempt no 1 with MAIN:  failed
 *         --b.    Connection attempt no 1 with DR1:  failed
 *         --c.     Connection attempt no 1 with DR2:  failed
 *         --d.    Connection attempt no 2 with MAIN:  failed
 *         --e.     Connection attempt no 2 with DR1:  failed
 *         --f.     Connection attempt no 2 with DR2:  failed
 *         --g.    Connection attempt no 3 with MAIN:  failed
 *         --h.     Connection attempt no 3 with DR1:  failed
 *         --i.      Connection attempt no 3 with DR2:  failed   =>  Attempt 3 finished. Fatal Failure is raised
 */
public class FatalFailureConnectionStrategy implements ConnectionStrategy {

    @Override
    public SocketChannel connect(String name,
                                 SocketAddressSupplier socketAddressSupplier,
                                 NetworkStatsListener<NetworkContext> networkStatsListener) {
        throw new UnsupportedOperationException("todo");
    }
}
