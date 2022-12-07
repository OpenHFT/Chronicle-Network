package net.openhft.chronicle.network.internal.cluster.handlers;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.RejectedExecutionException;

/**
 * An immutable {@link SubHandler} that does nothing on read or write
 *
 * @param <T>
 */
public final class NoOpHandler<T extends NetworkContext<T>> implements SubHandler<T> {

    private static final NoOpHandler<?> INSTANCE = new NoOpHandler<>();

    private NoOpHandler() {
        // Just to make it private, it's a singleton
    }

    public static <T extends NetworkContext<T>> SubHandler<T> instance() {
        return (NoOpHandler<T>) INSTANCE;
    }

    @Override
    public void cid(long cid) {
        // Do nothing, it's stateless & shared
    }

    @Override
    public long cid() {
        return -1;
    }

    @Override
    public void csp(@NotNull String cspText) {
        // Do nothing, it's stateless & shared
    }

    @Override
    public String csp() {
        return "no-csp";
    }

    @Override
    public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
        // Do nothing
    }

    @Override
    public Closeable closable() {
        return null;
    }

    @Override
    public void remoteIdentifier(int remoteIdentifier) {
        // Do nothing, it's stateless & shared
    }

    @Override
    public void localIdentifier(int localIdentifier) {
        // Do nothing, it's stateless & shared
    }

    @Override
    public void onInitialize(WireOut outWire) throws RejectedExecutionException {
        // Do nothing
    }

    @Override
    public void closeable(Closeable closeable) {
        // Do nothing, it's stateless & shared
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public T nc() {
        return null;
    }

    @Override
    public void nc(T nc) {
        // Do nothing, it's stateless & shared
    }
}
