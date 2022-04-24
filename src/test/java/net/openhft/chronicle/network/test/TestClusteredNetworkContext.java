package net.openhft.chronicle.network.test;

import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.cluster.VanillaClusteredNetworkContext;
import org.apache.mina.util.IdentityHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Set;

/**
 * A very minimal {@link net.openhft.chronicle.network.cluster.ClusteredNetworkContext} implementation for use in tests
 */
public class TestClusteredNetworkContext extends VanillaClusteredNetworkContext<TestClusteredNetworkContext, TestClusterContext> {

    public final Set<ConnectionListener> connectionListeners = new IdentityHashSet<>();
    private final ConnectionListener compositeConnectionListener = new CompositeConnectionListener(connectionListeners);
    private Long heartbeatTimeoutMsOverride;
    private boolean disableReconnect = false;
    private boolean returnNullConnectionListener = false;

    public TestClusteredNetworkContext(@NotNull TestClusterContext clusterContext) {
        super(clusterContext);
    }

    /**
     * Disable the reconnection, calls to {@link #socketReconnector()} will return a no-op {@link Runnable}
     */
    public void disableReconnect() {
        disableReconnect = true;
    }

    /**
     * This will override the NetworkContext#heartbeatTimeoutMs that's set on this context
     *
     * @param heartbeatTimeoutMsOverride The desired return value for heartbeatTimeoutMs
     */
    public void heartbeatTimeoutMsOverride(Long heartbeatTimeoutMsOverride) {
        this.heartbeatTimeoutMsOverride = heartbeatTimeoutMsOverride;
    }

    /**
     * This will cause the context to return a null ConnectionListener
     */
    public void returnNullConnectionListener() {
        this.returnNullConnectionListener = true;
    }

    @Override
    public Runnable socketReconnector() {
        if (disableReconnect) {
            return () -> {
            };
        }
        return super.socketReconnector();
    }

    @Override
    public long heartbeatTimeoutMs() {
        if (heartbeatTimeoutMsOverride != null) {
            return heartbeatTimeoutMsOverride;
        }
        return super.heartbeatTimeoutMs();
    }

    @Override
    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    @Override
    public void removeConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.remove(connectionListener);
    }

    @Override
    public @Nullable ConnectionListener acquireConnectionListener() {
        return returnNullConnectionListener ? null : compositeConnectionListener;
    }

    static class CompositeConnectionListener implements ConnectionListener {
        private final Collection<ConnectionListener> listenerCollection;

        public CompositeConnectionListener(Collection<ConnectionListener> listenerCollection) {
            this.listenerCollection = listenerCollection;
        }

        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            listenerCollection.forEach(cl -> cl.onConnected(localIdentifier, remoteIdentifier, isAcceptor));
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            listenerCollection.forEach(cl -> cl.onDisconnected(localIdentifier, remoteIdentifier, isAcceptor));
        }
    }
}
