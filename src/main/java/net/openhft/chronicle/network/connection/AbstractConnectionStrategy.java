package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.VanillaClientConnectionMonitor;
import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static net.openhft.chronicle.network.NetworkUtil.TCP_BUFFER_SIZE;

public abstract class AbstractConnectionStrategy extends AbstractMarshallableCfg implements ConnectionStrategy {
    protected int tcpBufferSize = Jvm.getInteger("tcp.client.buffer.size", TCP_BUFFER_SIZE);
    private final transient AtomicBoolean isClosed = new AtomicBoolean(false);
    protected ClientConnectionMonitor clientConnectionMonitor = new VanillaClientConnectionMonitor();
    protected String localSocketBindingHost;
    protected int localSocketBindingPort = 0;
    protected String localBindingNetworkInterface;
    protected ProtocolFamily localBindingProtocolFamily;

    @Override
    public @Nullable InetSocketAddress localSocketBinding() throws SocketException, UnknownHostException, IllegalStateException {
        if (localSocketBindingHost != null && localBindingNetworkInterface != null) {
            Jvm.warn().on(AbstractConnectionStrategy.class, "You have specified both localSocketBindingHost and localBindingNetworkInterface, using localSocketBindingHost");
        }

        if (localSocketBindingHost != null) {
            return getSocketBindingForHostName();
        }

        if (localBindingNetworkInterface != null) {
            return getSocketBindingForInterface();
        }

        return null;
    }

    @NotNull
    private InetSocketAddress getSocketBindingForHostName() throws UnknownHostException {
        final List<InetAddress> permittedAddresses = Arrays.stream(InetAddress.getAllByName(localSocketBindingHost))
                .filter(this::addressPermittedByProtocolFamily)
                .collect(Collectors.toList());
        if (permittedAddresses.size() > 1) {
            Jvm.warn().on(AbstractConnectionStrategy.class,
                    "Multiple eligible addresses available for hostname/protocol "
                            + localSocketBindingHost + "/" + protocolFamilyAsString()
                            + " (" + permittedAddresses + "), using " + permittedAddresses.get(0));
        } else if (permittedAddresses.isEmpty()) {
            throw new IllegalStateException("None of addresses for hostname " + localSocketBindingHost +
                    " available for binding for protocol " + protocolFamilyAsString());
        }
        return new InetSocketAddress(permittedAddresses.get(0), localSocketBindingPort);
    }

    @NotNull
    private InetSocketAddress getSocketBindingForInterface() throws SocketException {
        List<InetAddress> permittedAddresses = permittedAddressesForInterface();
        if (permittedAddresses.size() > 1) {
            Jvm.warn().on(AbstractConnectionStrategy.class,
                    "Multiple eligible addresses available on interface/protocol "
                            + localBindingNetworkInterface + "/" + protocolFamilyAsString()
                            + " (" + permittedAddresses + "), using " + permittedAddresses.get(0));
        } else if (permittedAddresses.isEmpty()) {
            throw new IllegalStateException("None of addresses from interface " + localBindingNetworkInterface +
                    " available for binding for protocol " + protocolFamilyAsString());
        }
        return new InetSocketAddress(permittedAddresses.get(0), localSocketBindingPort);
    }

    private String protocolFamilyAsString() {
        return localBindingProtocolFamily != null ? localBindingProtocolFamily.toString() : "ANY";
    }

    private List<InetAddress> permittedAddressesForInterface() throws SocketException {
        final NetworkInterface networkInterface = NetworkInterface.getByName(localBindingNetworkInterface);
        if (networkInterface == null) {
            throw new IllegalStateException("No matching interface found for name " + localBindingNetworkInterface + ", available interfaces: " + getAvailableInterfaceNames());
        }
        List<InetAddress> permittedAddresses = new ArrayList<>();
        Enumeration<InetAddress> addressEnumeration = networkInterface.getInetAddresses();
        while (addressEnumeration.hasMoreElements()) {
            InetAddress address = addressEnumeration.nextElement();
            if (addressPermittedByProtocolFamily(address))
                permittedAddresses.add(address);
        }
        return permittedAddresses;
    }

    private String getAvailableInterfaceNames() throws SocketException {
        StringBuilder stringBuilder = new StringBuilder();
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        boolean first = true;
        while (networkInterfaces.hasMoreElements()) {
            if (!first) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(networkInterfaces.nextElement().getName());
            first = false;
        }
        return stringBuilder.toString();
    }

    private boolean addressPermittedByProtocolFamily(InetAddress address) {
        if (localBindingProtocolFamily == null)
            return true;

        if (localBindingProtocolFamily == StandardProtocolFamily.INET)
            return address instanceof Inet4Address;

        if (localBindingProtocolFamily == StandardProtocolFamily.INET6)
            return address instanceof Inet6Address;

        return false; // Unsupported family
    }

    /**
     * Sets address for local connection sockets binding. Takes precedence over {@link #localBindingNetworkInterface(String)}
     *
     * @see #localBindingProtocolFamily(ProtocolFamily)
     */
    public void localSocketBindingHost(String localSocketBindingHost) {
        this.localSocketBindingHost = localSocketBindingHost;
    }

    public void localSocketBindingPort(int localSocketBindingPort) {
        this.localSocketBindingPort = localSocketBindingPort;
    }

    /**
     * Sets network interface name whose first matching address will be used for local connection sockets binding.
     *
     * @param localBindingNetworkInterface Network interface to use
     * @see #localBindingProtocolFamily(ProtocolFamily)
     */
    public void localBindingNetworkInterface(String localBindingNetworkInterface) {
        this.localBindingNetworkInterface = localBindingNetworkInterface;
    }

    /**
     * Sets protocol family to choose address from in case binding address or network interface correspond to more than
     * one address.
     *
     * @param localBindingProtocolFamily Protocol family to use
     */
    public void localBindingProtocolFamily(ProtocolFamily localBindingProtocolFamily) {
        this.localBindingProtocolFamily = localBindingProtocolFamily;
    }

    @Override
    public AbstractConnectionStrategy open() {
        isClosed.set(false);
        return this;
    }

    @Override
    public void close() {
        isClosed.set(true);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    protected long defaultMinPauseSec() {
        return ConnectionStrategy.super.minPauseSec();
    }

    protected long defaultMaxPauseSec() {
        return ConnectionStrategy.super.maxPauseSec();
    }
}
