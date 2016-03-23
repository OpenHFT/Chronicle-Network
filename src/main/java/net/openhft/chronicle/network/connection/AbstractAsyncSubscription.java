/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public abstract class AbstractAsyncSubscription implements AsyncSubscription {

    private final long tid;
    @NotNull
    private final TcpChannelHub hub;
    private final String csp;
    private final String name;

    public AbstractAsyncSubscription(@NotNull final TcpChannelHub hub, String csp, String name) {
        tid = hub.nextUniqueTransaction(Time.currentTimeMillis());
        this.hub = hub;
        this.csp = csp;
        this.name = name;
    }

    public AbstractAsyncSubscription(@NotNull final TcpChannelHub hub, String csp, byte identifier, String name) {
        this.tid = hub.nextUniqueTransaction(Time.currentTimeMillis()) * identifier;
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
        hub.writeMetaDataForKnownTID(tid(), hub.outWire(), csp, 0);
        hub.outWire().writeDocument(false, this::onSubscribe);
        hub.writeSocket(hub.outWire(), false); // will retry after reconnect.
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
