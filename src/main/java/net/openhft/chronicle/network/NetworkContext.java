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

public interface NetworkContext<T extends NetworkContext> {

    T isAcceptor(boolean serverSocket);

    boolean isAcceptor();

    T isUnchecked(boolean isUnchecked);

    boolean isUnchecked();

    T heartbeatIntervalTicks(long heartBeatIntervalTicks);

    long heartbeatIntervalTicks();

    T heartBeatTimeoutTicks(long heartBeatTimeoutTicks);

    long heartBeatTimeoutTicks();

    T socketChannel(SocketChannel sc);

    SocketChannel socketChannel();

    WireOutPublisher wireOutPublisher();

    void wireOutPublisher(WireOutPublisher wireOutPublisher);

    WireType wireType();

    T wireType(WireType wireType);

    SessionDetailsProvider sessionDetails();

    T sessionDetails(SessionDetailsProvider sessionDetails);


    /**
     * called when ever a connection is closed
     */
    void closeTask(Closeable r);

    Closeable closeTask();

    void connectionClosed(boolean isClosed);
}
