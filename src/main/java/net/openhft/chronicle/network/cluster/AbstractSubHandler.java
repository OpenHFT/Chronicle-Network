/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractSubHandler<T extends NetworkContext<T>> implements SubHandler<T> {
    private Closeable closeable;
    private T nc;
    private long cid;
    private String csp;
    private int remoteIdentifier;
    private int localIdentifier;
    private volatile boolean isClosed;

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
    public abstract void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire);

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
        nc().wireOutPublisher().publish(event);
    }

    @Override
    public void localIdentifier(int localIdentifier) {
        this.localIdentifier = localIdentifier;
    }

    public int localIdentifier() {
        return localIdentifier;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
