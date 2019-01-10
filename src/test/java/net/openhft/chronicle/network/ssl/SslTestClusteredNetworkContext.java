package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.cluster.Cluster;
import net.openhft.chronicle.network.cluster.ClusteredNetworkContext;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public final class SslTestClusteredNetworkContext
        extends VanillaNetworkContext implements ClusteredNetworkContext, SslNetworkContext {
    private final byte hostId;
    private final Cluster cluster;
    private final EventLoop eventLoop;

    SslTestClusteredNetworkContext(final byte hostId, final Cluster cluster, final EventLoop eventLoop) {
        this.hostId = hostId;
        this.cluster = cluster;
        this.eventLoop = eventLoop;
    }

    @Override
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @Override
    public byte getLocalHostIdentifier() {
        return hostId;
    }

    @Override
    public boolean isValidCluster(final String clusterName) {
        return true;
    }

    @Override
    public Cluster getCluster(final String clusterName) {
        return cluster;
    }

    @Override
    public SSLContext sslContext() {
        try {
            return SSLContextLoader.getInitialisedContext();
        } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException |
                IOException | KeyManagementException | UnrecoverableKeyException e) {
            throw new RuntimeException("Failed to load ssl context", e);
        }
    }

    @Override
    public NetworkStatsListener<? extends NetworkContext> networkStatsListener() {
        return new NetworkStatsListener<NetworkContext>() {
            @Override
            public void networkContext(final NetworkContext networkContext) {

            }

            @Override
            public void onNetworkStats(final long writeBps, final long readBps, final long socketPollCountPerSecond) {

            }

            @Override
            public void onHostPort(final String hostName, final int port) {

            }

            @Override
            public void onRoundTripLatency(final long nanosecondLatency) {

            }

            @Override
            public void close() {

            }
        };
    }
}
