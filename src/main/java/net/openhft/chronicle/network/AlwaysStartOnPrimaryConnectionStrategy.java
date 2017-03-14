package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.connection.FatalFailureMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;

/**
 * Loops through all the hosts:ports ( in order ) starting at the primary, till if finds a host that it can connect to.
 * If later, this successful connection is dropped, it will always return to the primary to begin attempting to find a successful connection,
 * If all the host:ports have been attempted since the last connection was established, no successful connection can be found,
 * then null is returned, and the fatalFailureMonitor.onFatalFailure() is triggered
 */
public class AlwaysStartOnPrimaryConnectionStrategy implements ConnectionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AlwaysStartOnPrimaryConnectionStrategy.class);

    private int tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", TCP_BUFFER);
    private int pausePeriodMs = Integer.getInteger("client.timeout", 500);

    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        Wires.readMarshallable(this, wire, false);
    }

    @Nullable
    @Override
    public SocketChannel connect(@NotNull String name,
                                 @NotNull SocketAddressSupplier socketAddressSupplier,
                                 @Nullable NetworkStatsListener<? extends NetworkContext> networkStatsListener,
                                 boolean didLogIn,
                                 @Nullable FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {


        if (socketAddressSupplier.get() == null || didLogIn)
            socketAddressSupplier.resetToPrimary();
        else
            socketAddressSupplier.failoverToNextAddress();

        for (; ; ) {

            SocketChannel socketChannel = null;
            try {

                @Nullable final InetSocketAddress socketAddress = socketAddressSupplier.get();
                if (socketAddress == null) {
                    Jvm.warn().on(AlwaysStartOnPrimaryConnectionStrategy.class, "failed to obtain socketAddress");
                    // at end
                    if (isAtEnd(socketAddressSupplier)) {
                        fatalFailureMonitor.onFatalFailure(name, "Failed to connect to any of these servers=" + socketAddressSupplier.remoteAddresses());
                        return null;
                    }
                    socketAddressSupplier.failoverToNextAddress();
                    continue;
                }

                socketChannel = openSocketChannel(socketAddress, tcpBufferSize, 500);

                if (socketChannel == null) {
                    Jvm.debug().on(getClass(), "unable to connected to " + socketAddressSupplier.toString());

                    // at end
                    if (isAtEnd(socketAddressSupplier)) {
                        fatalFailureMonitor.onFatalFailure(name, "Failed to connect to any of these servers=" + socketAddressSupplier.remoteAddresses());
                        return null;
                    }

                    socketAddressSupplier.failoverToNextAddress();
                    continue;
                }

                Jvm.debug().on(getClass(), "successfully connected to " + socketAddressSupplier.toString());

                if (networkStatsListener != null)
                    networkStatsListener.onHostPort(socketAddress.getHostString(), socketAddress.getPort());

                // success
                return socketChannel;

            } catch (Throwable e) {
                //noinspection ConstantConditions
                if (socketChannel != null)
                    closeQuietly(socketChannel);

                if (Jvm.isDebug())
                    LOG.info("", e);

                socketAddressSupplier.failoverToNextAddress();
                Time.parkNanos(TimeUnit.MILLISECONDS.toNanos(pausePeriodMs));
            }
        }
    }

    private boolean isAtEnd(SocketAddressSupplier socketAddressSupplier) {
        return socketAddressSupplier.size() - 1 == socketAddressSupplier.index();
    }

}
