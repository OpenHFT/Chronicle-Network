package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.cluster.WritableSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.openhft.chronicle.network.connection.CoreFields.csp;

public abstract class CspTcpHandler<T extends NetworkContext> extends WireTcpHandler<T> {

    protected final List<WriteMarshallable> writers = new ArrayList<>();
    @NotNull
    private final Map<Long, SubHandler> cidToHandle = new HashMap<>();
    private final Map<Object, SubHandler> registry = new HashMap<>();
    @Nullable
    private SubHandler handler;
    @Nullable
    private HeartbeatEventHandler heartbeatEventHandler;
    private long lastCid;

    @Nullable
    protected SubHandler handler() {
        return handler;
    }

    @Override
    public void close() {
        cidToHandle.values().forEach(Closeable::closeQuietly);
        super.close();
    }

    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     *
     * @return {@code true} if if a csp was read rather than a cid
     */
    protected boolean readMeta(@NotNull final WireIn wireIn) {
        final StringBuilder event = Wires.acquireStringBuilder();

        @NotNull ValueIn valueIn = wireIn.readEventName(event);

        if (csp.contentEquals(event)) {
            @Nullable final String csp = valueIn.text();

            long cid;
            event.setLength(0);
            valueIn = wireIn.readEventName(event);

            if (CoreFields.cid.contentEquals(event))
                cid = valueIn.int64();
            else
                throw new IllegalStateException("expecting 'cid' but eventName=" + event);

            event.setLength(0);
            valueIn = wireIn.readEventName(event);

            if (CoreFields.handler.contentEquals(event)) {
                if (cidToHandle.containsKey(cid)) {
                    String registeredCsp = cidToHandle.get(cid).csp();
                    if (!csp.equals(registeredCsp))
                        Jvm.warn().on(getClass(), "cid: " + cid + " already has handler registered with different csp, registered csp:" + registeredCsp + ", received csp: " + csp);
                    // already has it registered
                    return false;
                }
                handler = valueIn.typedMarshallable();
                handler.nc(nc());
                handler.closeable(this);

                try {
                    if (handler instanceof Registerable) {
                        Registerable registerable = (Registerable) this.handler;
                        registry.put(registerable.registryKey(), this.handler);
                        registerable.registry(registry);
                    }

                } catch (Exception e) {
                    Jvm.warn().on(getClass(), e);
                }

                if (handler instanceof ConnectionListener)
                    nc().addConnectionListener((ConnectionListener) handler);

                if (handler() instanceof HeartbeatEventHandler) {
                    assert heartbeatEventHandler == null : "its assumed that you will only have a " +
                            "single heartbeatReceiver per connection";
                    heartbeatEventHandler = (HeartbeatEventHandler) handler();
                }

                handler.cid(cid);
                handler.csp(csp);
                lastCid = cid;
                cidToHandle.put(cid, handler);

                if (handler instanceof WritableSubHandler)
                    writers.add(((WritableSubHandler) handler).writer());
            } else
                throw new IllegalStateException("expecting 'cid' but eventName=" + event);
            return true;
        } else if (CoreFields.cid.contentEquals(event)) {
            final long cid = valueIn.int64();
            if (cid == lastCid)
                return false;
            lastCid = cid;
            handler = cidToHandle.get(cid);

            if (handler == null) {
                throw new IllegalStateException("handler not found : for CID=" + cid + ", " +
                        "known cids=" + cidToHandle.keySet());
            }
        } else {
            throw new IllegalStateException("expecting either csp or cid, event=" + event);
        }

        return false;
    }

    @Nullable
    protected HeartbeatEventHandler heartbeatEventHandler() {
        return heartbeatEventHandler;
    }
}

