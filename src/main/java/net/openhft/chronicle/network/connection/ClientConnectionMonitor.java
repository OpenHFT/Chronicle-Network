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

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface ClientConnectionMonitor {

    /**
     * Call just after the client as successfully established a connection to the server
     *
     * @param name          the name of the connection
     * @param socketAddress the address that we have just connected to
     */
    void onConnected(@Nullable String name, @NotNull SocketAddress socketAddress);

    /**
     * call just after the client has disconnect to the server, this maybe called as part of a
     * failover
     *
     * @param name          the name of the connection
     * @param socketAddress the address of the socket that we have been disconnected from
     */
    void onDisconnected(@Nullable String name, @NotNull SocketAddress socketAddress);
}
