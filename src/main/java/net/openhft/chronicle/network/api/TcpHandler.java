/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network.api;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.ClientClosedProvider;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter.lawrey on 22/01/15.
 */
@FunctionalInterface
public interface TcpHandler extends ClientClosedProvider, Closeable {

    /**
     * The server reads the bytes {@code in} from the client and sends a response {@code out} back
     * to the client.
     *  @param in             the bytes send from the client
     * @param out            the response send back to the client
     */
    void process(@NotNull Bytes in, @NotNull Bytes out);

    default void sendHeartBeat(Bytes out, SessionDetailsProvider sessionDetails) {
    }

    default void onEndOfConnection(boolean heartbeatTimeOut) {
    }

    @Override
    default void close() {
    }

    default void onReadTime(long readTimeNS) {
    }

    default void onWriteTime(long writeTimeNS) {
    }

}
