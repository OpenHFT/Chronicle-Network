/*
 * Copyright 2016-2020 http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.cluster.handlers;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.api.session.WritableSubHandler;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.internal.cluster.handlers.NoOpHandler;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static net.openhft.chronicle.network.connection.CoreFields.csp;

public abstract class CspTcpHandler<T extends NetworkContext<T>> extends WireTcpHandler<T> {

    // because close() is called in another thread this must be a concurrent structure
    protected final List<WritableSubHandler<T>> writers = new CopyOnWriteArrayList<>();

    @NotNull
    private final TLongObjectMap<SubHandler<T>> cidToHandle = new TLongObjectHashMap<>();
    private final Map<Object, SubHandler<T>> registry = new HashMap<>();
    @NotNull
    private final TLongObjectMap<String> removedHandlers = new TLongObjectHashMap<>();
    @Nullable
    private SubHandler<T> handler;
    @Nullable
    private HeartbeatEventHandler heartbeatEventHandler;
    private long lastCid;

    @Nullable
    protected SubHandler<T> handler() {
        return handler;
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(cidToHandle.values());
        cidToHandle.clear();
        super.performClose();
    }

    protected void removeHandler(SubHandler<T> handler) {
        cidToHandle.remove(handler.cid());
        if (handler instanceof WritableSubHandler) {
            writers.remove(handler);
        }
        if (handler instanceof Registerable) {
            registry.remove(((Registerable<?>) handler).registryKey());
        }
        if (handler instanceof ConnectionListener) {
            nc().removeConnectionListener((ConnectionListener) handler);
        }
        if (this.handler == handler) {
            this.handler = NoOpHandler.instance();
            this.lastCid = 0;
        }
        removedHandlers.put(handler.cid(), handler.csp());
        Closeable.closeQuietly(handler);
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
                    if (!registeredCsp.equals(csp))
                        Jvm.warn().on(getClass(), "cid: " + cid + " already has handler registered with different csp, registered csp:" + registeredCsp + ", received csp: " + csp);
                    // already has it registered
                    return false;
                }
                try {
                    handler = valueIn.typedMarshallable();
                    handler.nc(nc());
                } catch (RejectedHandlerException ex) {
                    Jvm.warn().on(getClass(), "Handler for csp=" + csp + ", cid=" + cid + " was rejected: " + ex.getMessage(), ex);
                    handler = null;
                    lastCid = cid;
                    return false;
                }
                handler.closeable(this);

                try {
                    if (handler instanceof Registerable) {
                        final Registerable<SubHandler<T>> registerable = (Registerable<SubHandler<T>>) this.handler;
                        registry.put(registerable.registryKey(), this.handler);
                        registerable.registry(registry);
                    }
                } catch (Exception e) {
                    Jvm.warn().on(getClass(), e);
                }

                if (handler instanceof ConnectionListener)
                    nc().addConnectionListener((ConnectionListener) handler);

                if (handler instanceof HeartbeatEventHandler) {
                    assert heartbeatEventHandler == null : "its assumed that you will only have a " +
                            "single heartbeatReceiver per connection";
                    heartbeatEventHandler = (HeartbeatEventHandler) handler;
                }

                handler.cid(cid);
                handler.csp(csp);
                lastCid = cid;
                cidToHandle.put(cid, handler);
                removedHandlers.remove(cid);

                if (handler instanceof WritableSubHandler)
                    writers.add(((WritableSubHandler<T>) handler));
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
                if (removedHandlers.containsKey(cid)) {
                    Jvm.warn().on(CspTcpHandler.class, "Installing no-op handler to drop messages for removed handler (cid=" + cid + ", csp=" + removedHandlers.get(cid) + ")");
                    handler = NoOpHandler.instance();
                } else {
                    throw new IllegalStateException("handler not found : for CID=" + cid + ", " +
                            "known cids=" + cidToHandle.keySet());
                }
            }
        } else {
            throw new IllegalStateException("expecting either csp or cid, event=" + event);
        }

        return false;
    }

    protected long lastCid() {
        return lastCid;
    }

    @Nullable
    protected HeartbeatEventHandler heartbeatEventHandler() {
        return heartbeatEventHandler;
    }
}

