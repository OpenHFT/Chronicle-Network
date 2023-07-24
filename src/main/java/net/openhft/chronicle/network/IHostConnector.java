package net.openhft.chronicle.network;

import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;

import java.io.Closeable;

/**
 * Just prefixed with I to not confuse the git diff, will rename to HostConnector in a second commit
 */
public interface IHostConnector<T extends ClusteredNetworkContext<T>, C extends ClusterContext<C, T>> extends Closeable {

    /**
     * Start connecting to the remote host
     */
    void connect();
}
