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

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.TerminationEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T> {

    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private boolean isUnchecked;


    private SessionDetailsProvider sessionDetails;
    private boolean connectionClosed;
    private Closeable closeTask;

    @Nullable
    private TerminationEventHandler terminationEventHandler;

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
    public boolean isUnchecked() {
        return isUnchecked;
    }

    WireOutPublisher wireOutPublisher;


    @Override
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public void wireOutPublisher(WireOutPublisher wireOutPublisher) {
        this.wireOutPublisher = wireOutPublisher;
    }


    WireType wireType = WireType.TEXT;

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


    @Override
    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    @Override
    public void terminationEventHandler(@Nullable TerminationEventHandler terminationEventHandler) {
        this.terminationEventHandler = terminationEventHandler;
    }

    AtomicLong uniqueCspid = new AtomicLong();


    public long createUniqueCid() {
        // todo maybe also add the host id factor to this to ensure better uniqueness
        long time = System.currentTimeMillis();

        for (; ; ) {

            final long current = this.uniqueCspid.get();

            if (time == this.uniqueCspid.get()) {
                time++;
                continue;
            }

            final boolean success = this.uniqueCspid.compareAndSet(current, time);

            if (!success)
                continue;

            return time;
        }
    }
}
