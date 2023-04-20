package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.VanillaClientConnectionMonitor;
import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import org.jetbrains.annotations.Nullable;

import java.net.*;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;

public abstract class AbstractConnectionStrategy extends AbstractMarshallableCfg implements ConnectionStrategy {
    protected int tcpBufferSize = Jvm.getInteger("tcp.client.buffer.size", TCP_BUFFER);
    private final transient AtomicBoolean isClosed = new AtomicBoolean(false);
    protected ClientConnectionMonitor clientConnectionMonitor = new VanillaClientConnectionMonitor();
    protected String localSocketBindingHost;
    protected int localSocketBindingPort = 0;
    protected String localBindingNetworkInterface;
    protected ProtocolFamily localBindingProtocolFamily;

    @Override
    public @Nullable InetSocketAddress localSocketBinding() throws SocketException, UnknownHostException, IllegalStateException {
        if (localSocketBindingHost != null) {
            for (InetAddress address : InetAddress.getAllByName(localSocketBindingHost)) {
                if (addressPermittedByProtocolFamily(address))
                    return new InetSocketAddress(address, localSocketBindingPort);
            }

            throw new IllegalStateException("None of addresses from " + localSocketBindingHost +
                    " available for binding for protocol " + (localBindingProtocolFamily != null ? localBindingProtocolFamily : "ANY"));
        }

        if (localBindingNetworkInterface != null) {
            Enumeration<InetAddress> addressEnumeration = NetworkInterface.getByName(localBindingNetworkInterface).getInetAddresses();
            while (addressEnumeration.hasMoreElements()) {
                InetAddress address = addressEnumeration.nextElement();
                if (addressPermittedByProtocolFamily(address))
                    return new InetSocketAddress(address, localSocketBindingPort);
            }

            throw new IllegalStateException("None of addresses from interface " + localBindingNetworkInterface +
                    " available for binding for protocol " + (localBindingProtocolFamily != null ? localBindingProtocolFamily : "ANY"));
        }

        return null;
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
     * @see #localBindingProtocolFamily(ProtocolFamily)
     *
     * @param localBindingNetworkInterface Network interface to use
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
