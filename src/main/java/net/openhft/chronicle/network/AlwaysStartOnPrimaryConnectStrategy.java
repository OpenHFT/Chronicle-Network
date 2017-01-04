package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static net.openhft.chronicle.core.Jvm.pause;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;

/**
 * loops through all the host, till if finds a host that it can connect to.
 * If an established connected is dropped, will always return to the primary to begin attempting to find a successful connection,
 */
public class AlwaysStartOnPrimaryConnectStrategy implements ConnectionStrategy {

    private int tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", TCP_BUFFER);

    private static final Logger LOG = LoggerFactory.getLogger(AlwaysStartOnPrimaryConnectStrategy.class);

    @Nullable
    public SocketChannel connect(String name,
                                 SocketAddressSupplier socketAddressSupplier,
                                 NetworkStatsListener<NetworkContext> networkStatsListener) {

        socketAddressSupplier.resetToPrimary();

        long start = System.currentTimeMillis();

        for (; ; ) {

            if (start + socketAddressSupplier.timeoutMS() < System.currentTimeMillis()) {

                @NotNull String oldAddress = socketAddressSupplier.toString();

                // fatal failure we have attempted all the host
                if (isAtEnd(socketAddressSupplier))
                    return null;

                socketAddressSupplier.failoverToNextAddress();

                if ("(none)".equals(oldAddress)) {
                    LOG.info("Connection Dropped to address=" +
                            oldAddress + ", so will fail over to" +
                            socketAddressSupplier + ", name=" + name);
                }

                if (socketAddressSupplier.get() == null) {
                    Jvm.warn().on(getClass(), "failed to establish a socket " +
                            "connection of any of the following servers=" +
                            socketAddressSupplier.all() + " so will re-attempt");
                    socketAddressSupplier.resetToPrimary();
                }

                // reset the timer, so that we can try this new address for a while
                start = System.currentTimeMillis();
            }
            SocketChannel socketChannel = null;
            try {

                @Nullable final InetSocketAddress socketAddress = socketAddressSupplier.get();
                if (socketAddress == null) {
                    Jvm.warn().on(AlwaysStartOnPrimaryConnectStrategy.class, "failed to obtain socketAddress");
                    continue;
                }

                socketChannel = openSocketChannel(socketAddress, tcpBufferSize);

                if (socketChannel == null) {
                    pause(1_000);
                    continue;
                }

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
                pause(1_000);
            }
        }
    }

    private boolean isAtEnd(SocketAddressSupplier socketAddressSupplier) {
        return socketAddressSupplier.size() - 1 == socketAddressSupplier.index();
    }
}
