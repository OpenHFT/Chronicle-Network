package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T> {

    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private boolean isUnchecked;

    private long heartBeatTimeoutTicks = 40_000;
    private long heartbeatIntervalTicks = 20_000;
    private SessionDetailsProvider sessionDetails;
    private boolean connectionClosed;

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public T socketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        return (T) this;
    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return
     */
    @Override
    public T isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
        return (T) this;
    }

    /**
     * @return {@code} true if its a server socket, {@code} false if its a client
     */
    @Override
    public boolean isAcceptor() {
        return isAcceptor;
    }

    @Override
    public T isUnchecked(boolean isUnchecked) {
        this.isUnchecked = isUnchecked;
        return (T) this;
    }

    @Override
    public boolean isUnchecked() {
        return isUnchecked;
    }

    @Override
    public T heartbeatIntervalTicks(long heartbeatIntervalTicks) {
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        return (T) this;
    }

    @Override
    public long heartbeatIntervalTicks() {
        return heartbeatIntervalTicks;
    }

    @Override
    public T heartBeatTimeoutTicks(long heartBeatTimeoutTicks) {
        this.heartBeatTimeoutTicks = heartBeatTimeoutTicks;
        return (T) this;
    }

    @Override
    public long heartBeatTimeoutTicks() {
        return heartBeatTimeoutTicks;
    }

    WireOutPublisher wireOutPublisher;

    @Nullable
    @Override
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public void wireOutPublisher(WireOutPublisher wireOutPublisher) {
        this.wireOutPublisher = wireOutPublisher;
    }

    WireType wireType = WireType.TEXT;

    @Override
    public WireType wireType() {
        return wireType;
    }

    public T wireType(WireType wireType) {
        this.wireType = wireType;
        return (T) this;
    }

    @Override
    public SessionDetailsProvider sessionDetails() {
        return this.sessionDetails;
    }

    @Override
    public T sessionDetails(SessionDetailsProvider sessionDetails) {
        this.sessionDetails = sessionDetails;
        return (T) this;
    }

    public boolean connectionClosed() {
        return this.connectionClosed;
    }

    public void connectionClosed(boolean connectionClosed) {
        this.connectionClosed = connectionClosed;
    }
}
