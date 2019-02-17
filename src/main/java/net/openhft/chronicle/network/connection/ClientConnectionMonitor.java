/*
 * Copyright 2016 higherfrequencytrading.com
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

package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.NotNull;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface ClientConnectionMonitor extends FatalFailureMonitor {

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
