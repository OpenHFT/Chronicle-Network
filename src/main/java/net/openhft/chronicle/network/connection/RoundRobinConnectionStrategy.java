package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.AlwaysStartOnPrimaryConnectionStrategy;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class RoundRobinConnectionStrategy extends AbstractConnectionStrategy {
    private int pausePeriodMs = Jvm.getInteger("client.timeout", 500);
    private int socketConnectionTimeoutMs = Jvm.getInteger("connectionStrategy.socketConnectionTimeoutMs", 1);
    private long pauseMillisBeforeReconnect = Jvm.getInteger("connectionStrategy.pauseMillisBeforeReconnect", 500);

    public RoundRobinConnectionStrategy() {
        this(DefaultOpenSocketStrategy.INSTANCE);
    }

    public RoundRobinConnectionStrategy(OpenSocketStrategy openSocketStrategy) {
        super(openSocketStrategy);
    }

    @Override
    public ChronicleSocketChannel connect(@NotNull String name,
                                          @NotNull SocketAddressSupplier socketAddressSupplier,
                                          @NotNull ConnectionState previousConnectionState,
                                          @Nullable FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {
        if (socketAddressSupplier.get() == null) {
            socketAddressSupplier.resetToPrimary();
        } else {
            socketAddressSupplier.failoverToNextAddress();
        }

        if (fatalFailureMonitor == null) {
            fatalFailureMonitor = FatalFailureMonitor.NO_OP;
        }

        final int firstIndex = socketAddressSupplier.index();

        for (; ; ) {
            throwExceptionIfClosed();
            ChronicleSocketChannel socketChannel = null;
            try {
                @Nullable final InetSocketAddress socketAddress = socketAddressSupplier.get();
                if (socketAddress == null) {
                    Jvm.warn().on(AlwaysStartOnPrimaryConnectionStrategy.class, "failed to obtain socketAddress");
                } else {
                    socketChannel = openSocketStrategy.openSocketChannel(this, socketAddress, tcpBufferSize, pausePeriodMs, socketConnectionTimeoutMs);
                    if (socketChannel != null && socketChannel.isOpen()) {
                        Jvm.debug().on(getClass(), "successfully connected to " + socketAddressSupplier);
                        return socketChannel;
                    }
                    if (Jvm.isDebugEnabled(getClass()))
                        Jvm.debug().on(getClass(), "unable to connected to " + socketAddressSupplier);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                if (socketChannel != null)
                    closeQuietly(socketChannel);
                Jvm.warn().on(RoundRobinConnectionStrategy.class, "Unexpected error connecting", e);
            }
            socketAddressSupplier.failoverToNextAddress();
            if (socketAddressSupplier.index() == firstIndex) {
                fatalFailureMonitor.onFatalFailure(name, "Failed to connect to any of these servers=" + socketAddressSupplier.remoteAddresses());
                return null;
            }
        }
    }

    @Override
    public long pauseMillisBeforeReconnect() {
        return pauseMillisBeforeReconnect;
    }

    public RoundRobinConnectionStrategy tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    public RoundRobinConnectionStrategy pausePeriodMs(int pausePeriodMs) {
        this.pausePeriodMs = pausePeriodMs;
        return this;
    }

    public RoundRobinConnectionStrategy socketConnectionTimeoutMs(int socketConnectionTimeoutMs) {
        this.socketConnectionTimeoutMs = socketConnectionTimeoutMs;
        return this;
    }

    public RoundRobinConnectionStrategy pauseMillisBeforeReconnect(long pauseMillisBeforeReconnect) {
        this.pauseMillisBeforeReconnect = pauseMillisBeforeReconnect;
        return this;
    }
}
