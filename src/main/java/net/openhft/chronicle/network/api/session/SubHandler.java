package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface SubHandler<T extends NetworkContext> extends NetworkContextManager<T>, Closeable {

    void cid(long cid);

    long cid();

    void csp(@NotNull String cspText);

    String csp();

    void processData(@NotNull WireIn inWire, @NotNull WireOut outWire);

    void remoteIdentifier(byte remoteIdentifier);

    /**
     * called after all the construction and configuration has completed
     *
     * @param outWire allow data to be written
     */
    void onBootstrap(WireOut outWire);
}
