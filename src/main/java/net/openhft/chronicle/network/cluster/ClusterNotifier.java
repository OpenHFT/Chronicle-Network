package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * @author Rob Austin.
 */
class ClusterNotifier<E extends HostDetails> implements TerminationEventHandler, ConnectionChangedNotifier {

    private final List<WriteMarshallable> bootstaps;
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final ConnectionChangedNotifier connectionEventManager;
    private final HostConnector hostConnector;

    <E extends HostDetails> ClusterNotifier(ConnectionChangedNotifier connectionEventManager,
                                            HostConnector hostConnector,
                                            List<WriteMarshallable> bootstaps) {
        this.connectionEventManager = connectionEventManager;
        this.hostConnector = hostConnector;
        this.bootstaps = bootstaps;
    }

    public void connect() {
        bootstaps.forEach(hostConnector::bootstrap);
        hostConnector.connect();
    }

    @Override
    public void onConnectionChanged(boolean isConnected, final NetworkContext nc) {

        if (!isConnected)
            onClose();

        connectionEventManager.onConnectionChanged(isConnected, nc);

    }

    private void onClose() {
        closeQuietly(hostConnector);

        // reconnect if not terminated
        if (terminated.get())
            return;

        // re-connect
        connect();
    }

    @Override
    public void onTerminate() {
        terminated.set(true);
        hostConnector.close();
    }


}
