package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public abstract class AbstractSubHandler<T extends NetworkContext> implements SubHandler<T> {
    private Closeable closeable;
    private T nc;
    private long cid;
    private String csp;
    private int remoteIdentifier;


    @Override
    public void cid(long cid) {
        this.cid = cid;
    }

    @Override
    public long cid() {
        return cid;
    }

    @Override
    public void csp(@NotNull String csp) {
        this.csp = csp;
    }

    @Override
    public String csp() {
        return this.csp;
    }

    @Override
    public abstract void processData(@NotNull WireIn inWire, @NotNull WireOut outWire);

    @Override
    public T nc() {
        return nc;
    }

    @Override
    public void closeable(Closeable closeable) {
        this.closeable = closeable;
    }

    @Override
    public Closeable closable() {
        return closeable;
    }

    @Override
    public void nc(T nc) {
        this.nc = nc;
    }

    public int remoteIdentifier() {
        return remoteIdentifier;
    }

    @Override
    public void remoteIdentifier(int remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
    }

    public void publish(WriteMarshallable event) {
        nc().wireOutPublisher().put("", event);
    }


}
