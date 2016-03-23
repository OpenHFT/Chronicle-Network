package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.RemoteConnector;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class HostConnector implements Closeable {

    private final WireType wireType;

    //  private final WriteMarshallable header;
    private final Function<WireType, WireOutPublisher> wireOutPublisherFactory;
    private final List<WriteMarshallable> bootstraps = new LinkedList<>();

    private final RemoteConnector remoteConnector;
    private final String connectUri;

    private WireOutPublisher wireOutPublisher;
    private NetworkContext nc;
    private long timeoutMs;
    private final Function<ClusterContext, NetworkContext> networkContextFactory;
    private final ClusterContext clusterContext;
    private volatile boolean isConnected;

    @NotNull
    private EventLoop eventLoop;

    HostConnector(@NotNull ClusterContext clusterContext,
                  final RemoteConnector remoteConnector,
                  final HostDetails hostdetails) {
        this.clusterContext = clusterContext;
        this.remoteConnector = remoteConnector;
        //final WriteMarshallable header = clusterContext.handlerFactory().apply(clusterContext,
        //        hostdetails);
        this.networkContextFactory = clusterContext.networkContextFactory();
        this.connectUri = hostdetails.connectUri();
        this.wireType = clusterContext.wireType();
        //  this.header = header;
        this.wireOutPublisherFactory = clusterContext.wireOutPublisherFactory();
        this.timeoutMs = clusterContext.connectionTimeoutMs();
        this.eventLoop = clusterContext.eventLoop();
        this.wireOutPublisher = wireOutPublisherFactory.apply(WireType.TEXT);
    }


    @Override
    public synchronized void close() {
        isConnected = false;
        Closeable.closeQuietly(wireOutPublisher);
        bootstraps.clear();
        if (nc.socketChannel() != null)
            Closeable.closeQuietly(nc.socketChannel());
        wireOutPublisher.clear();
    }

    public synchronized void bootstrap(WriteMarshallable subscription) {
        bootstraps.add(subscription);

        if (isConnected && wireOutPublisher != null)
            wireOutPublisher.put("", subscription);

    }


    public synchronized void connect() {

        isConnected = true;

        // we will send the initial header as text wire, then the rest will be sent in
        // what ever wire is configured
        nc = networkContextFactory.apply(clusterContext);
        nc.wireOutPublisher(wireOutPublisher);
        nc.wireType(wireType);
        nc.closeTask(this);

        wireOutPublisher.wireType(wireType);
        //  wireOutPublisher.put("", header);

        for (WriteMarshallable bootstrap : bootstraps) {
            wireOutPublisher.put("", bootstrap);
        }

        remoteConnector.connect(connectUri, eventLoop, nc, timeoutMs);
    }
}