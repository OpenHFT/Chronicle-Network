package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.ssl.SslDelegatingTcpHandler;
import net.openhft.chronicle.network.ssl.SslNetworkContext;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.Jvm.debug;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Repeatedly checks to see if a connection needs to be established or terminated and acts accordingly
 * <p>
 * Uses a {@link SocketAddressSupplier} and a {@link ConnectionStrategy} to determine how to connect
 */
public class ConnectorEventHandler<T extends NetworkContext<T>> extends AbstractCloseable implements EventHandler {

    private final Pauser pauser;
    private final ConnectionStrategy connectionStrategy;
    private final Supplier<T> networkContextFactory;
    private final String name;
    private final Supplier<Boolean> connecting;
    @NotNull
    private final ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory;
    private final SocketAddressSupplier socketAddressSupplier;
    private final Consumer<EventHandler> eventHandlerAdder;
    private ChronicleSocketChannel socketChannel;
    private T nc;

    public ConnectorEventHandler(String name,
                                 ConnectionStrategy connectionStrategy,
                                 Supplier<T> networkContextFactory,
                                 @NotNull ThrowingFunction<T, TcpEventHandler<T>, IOException> tcpEventHandlerFactory,
                                 Supplier<Boolean> connecting,
                                 @NotNull SocketAddressSupplier socketAddressSupplier,
                                 Consumer<EventHandler> eventHandlerAdder) {
        this.socketAddressSupplier = socketAddressSupplier;
        this.name = name;
        this.connecting = connecting;
        this.connectionStrategy = connectionStrategy;
        this.networkContextFactory = networkContextFactory;
        this.tcpEventHandlerFactory = tcpEventHandlerFactory;
        this.eventHandlerAdder = eventHandlerAdder;

        pauser = new LongPauser(0, 0, this.connectionStrategy.minPauseSec(), this.connectionStrategy.maxPauseSec(), TimeUnit.SECONDS);
        // eagerly load logger now to mitigate delays if it is used later
        debug().isEnabled(getClass());
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InvalidMarshallableException {
        if (isClosed()) {
            throw InvalidEventHandlerException.reusable();
        }

        if (socketChannel == null && connecting.get()) {

            if (pauser.asyncPausing()) {
                return false;
            }

            final ClientConnectionMonitor clientConnectionMonitor = connectionStrategy.clientConnectionMonitor();

            ChronicleSocketChannel newChannel = null;
            try {
                // TODO: what does this mean in the generic network context?
                final boolean didLogIn = false;
                newChannel = connectionStrategy.connect(name, socketAddressSupplier, didLogIn, clientConnectionMonitor);

            } catch (ClosedIllegalStateException e) {
                closeQuietly(this);
                throw new InvalidEventHandlerException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Jvm.warn().on(ConnectorEventHandler.class, "Interrupted while connecting", e);
            }

            if (newChannel == null) {
                // Should be a warning, but use INFO for now to allow running build-all
                Jvm.startup().on(ConnectorEventHandler.class, "Unable to connect to any of the hosts " + socketAddressSupplier.remoteAddresses());
                pauser.asyncPause();
                return false;
            }

            nc = networkContextFactory.get();

            NetworkStatsListener<T> networkStatsListener = nc.networkStatsListener();

            InetSocketAddress socketAddress = socketAddressSupplier.get();
            assert socketAddress != null;
            if (networkStatsListener != null) {
                networkStatsListener.onHostPort(socketAddress.getHostString(), socketAddress.getPort());
            }

            Jvm.debug().on(ConnectorEventHandler.class, "Successfully connected to " + socketAddressSupplier);

            nc.socketChannel(newChannel);

            final String connectionName = name;
            final InetSocketAddress address = getRemoteAddress(newChannel);
            if (clientConnectionMonitor != null) {
                clientConnectionMonitor.onConnected(name, address);
            }

            // if a connection failure to the primary had not logged in, it is not retried on fail-over
            nc.addCloseListener(() -> {

                if (clientConnectionMonitor != null)
                    clientConnectionMonitor.onDisconnected(connectionName, address);
//                hasLoggedInPreviously = nc.hasReceivedLoginResponse(); // TODO what to do about this
            });

            try {
                @NotNull final TcpEventHandler<T> tcpEventHandler = tcpEventHandlerFactory.apply(nc);
                socketChannel = tcpEventHandler.socketChannel();
                TcpHandler<T> tcpHandler = tcpEventHandler.tcpHandler();

                if (nc instanceof SslNetworkContext) {
                    SslNetworkContext<T> sslNc = (SslNetworkContext<T>) nc;
                    if (sslNc.sslContext() != null) {
                        sslNc.sslParameters(withSniHostName(sslNc.sslParameters(), socketAddress));

                        SslDelegatingTcpHandler frontHandler =
                                new SslDelegatingTcpHandler(eventHandlerAdder).delegate(tcpHandler);
                        tcpEventHandler.tcpHandler(frontHandler);
                    }
                }
                tcpEventHandler.exposeOutBufferToTcpHandler();

                eventHandlerAdder.accept(tcpEventHandler);
                tcpHandler.onConnected(nc);

            } catch (IOException e) {
                // Should be a warning, but use INFO for now to allow running build-all
                Jvm.startup().on(ConnectorEventHandler.class, "Error creating TCP event handler", e);
                socketChannel = null;
            }
        } else if (socketChannel != null && socketChannel.isClosed()) {
            closeQuietly(nc);
            nc = null;
            socketChannel = null;
            return true;
        }

        if (!isClosed()) {
            pauser.asyncPause();
        }

        return false;
    }

    private InetSocketAddress getRemoteAddress(ChronicleSocketChannel connect) {
        try {
            return connect.getRemoteAddress();
        } catch (IOException e) {
            Jvm.warn().on(getClass(), e);
            return null;
        }
    }

    @Override
    protected void performClose() throws IllegalStateException {

    }

    private SSLParameters withSniHostName(SSLParameters sslParameters, InetSocketAddress hostName) {
        if (hostName == null)
            return sslParameters;

        String hostString = hostName.getHostString();
        if (hostString.equals(hostName.getAddress().getHostAddress()) ||
                hostString.equalsIgnoreCase("localhost"))
            return sslParameters;

        if (sslParameters == null) {
            sslParameters = new SSLParameters();
        } else {
            if ((sslParameters.getSNIMatchers() != null && !sslParameters.getSNIMatchers().isEmpty()) ||
                    (sslParameters.getServerNames() != null && !sslParameters.getServerNames().isEmpty()))
                return sslParameters;
        }

        sslParameters.setServerNames(Collections.singletonList(new SNIHostName(hostString)));

        return sslParameters;
    }
}
