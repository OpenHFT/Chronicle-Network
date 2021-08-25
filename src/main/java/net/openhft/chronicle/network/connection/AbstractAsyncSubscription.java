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
package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
public abstract class AbstractAsyncSubscription implements AsyncSubscription {

    private final long tid;
    @NotNull
    private final TcpChannelHub hub;
    private final String csp;
    private final String name;

    protected AbstractAsyncSubscription(@NotNull final TcpChannelHub hub, String csp, String name) {
        tid = hub.nextUniqueTransaction(System.currentTimeMillis());
        this.hub = hub;
        this.csp = csp;
        this.name = name;
    }

    protected AbstractAsyncSubscription(@NotNull final TcpChannelHub hub, String csp, byte identifier, String name) {
        this.tid = hub.nextUniqueTransaction(System.currentTimeMillis()) * identifier;
        this.hub = hub;
        this.csp = csp;
        this.name = name;
    }

    @Override
    public long tid() {
        return tid;
    }

    @Override
    public void applySubscribe() {
        assert hub.outBytesLock().isHeldByCurrentThread();
        hub.writeMetaDataForKnownTID(tid(), hub.outWire(), csp, 0, false);
        hub.outWire().writeDocument(false, this::onSubscribe);
        hub.writeSocket(hub.outWire(), false, false); // will retry after reconnect.
    }

    /**
     * called when ever the  TcpChannelHub is ready to make a subscription
     *
     * @param wireOut the wire to write the subscription to
     */
    public abstract void onSubscribe(WireOut wireOut);

    /**
     * called whenever the connection to the server has been dropped
     */
    @Override
    public void onClose() {

    }

    @NotNull
    @Override
    public String toString() {
        return "AbstractAsyncSubscription{" +
                "name='" + name + '\'' +
                ", csp='" + csp + '\'' +
                ", tid=" + tid +
                '}';
    }
}
