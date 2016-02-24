package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;

import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T> {


    private SocketChannel socketChannel;
    private boolean isServerSocket = true;
    private boolean isUnchecked;
    private WireOutPublisher wireOutPublisher;
    private long heartBeatTimeoutTicks = 40_000;
    private long heartbeatIntervalTicks = 20_000;
    private SessionDetailsProvider sessionDetails;

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public T socketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        return (T) this;
    }

    @Override
    public T isServerSocket(boolean b) {
        this.isServerSocket = b;
        return (T) this;
    }

    @Override
    public boolean isServerSocket() {
        return isServerSocket;
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

    @Override
    public WireOutPublisher wireOutPublisher() {
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

}
