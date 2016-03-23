/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;

import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T> {

    WireOutPublisher wireOutPublisher;
    WireType wireType = WireType.TEXT;
    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private boolean isUnchecked;
    private long heartBeatTimeoutTicks = 40_000;
    private long heartbeatIntervalTicks = 20_000;
    private SessionDetailsProvider sessionDetails;
    private boolean connectionClosed;
    private Closeable closeTask;

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public T socketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        return (T) this;
    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return
     */
    @Override
    public T isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
        return (T) this;
    }

    /**
     * @return {@code} true if its a server socket, {@code} false if its a client
     */
    @Override
    public boolean isAcceptor() {
        return isAcceptor;
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
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public void wireOutPublisher(WireOutPublisher wireOutPublisher) {
        this.wireOutPublisher = wireOutPublisher;
    }

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

    @Override
    public void closeTask(Closeable closeTask) {
        this.closeTask = closeTask;
    }

    @Override
    public Closeable closeTask() {
        return closeTask;
    }

    public boolean connectionClosed() {
        return this.connectionClosed;
    }

    @Override
    public void connectionClosed(boolean connectionClosed) {
        this.connectionClosed = connectionClosed;
    }
}
