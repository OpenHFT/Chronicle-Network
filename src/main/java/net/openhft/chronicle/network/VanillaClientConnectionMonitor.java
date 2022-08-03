/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;

import static net.openhft.chronicle.core.Jvm.debug;

public class VanillaClientConnectionMonitor extends AbstractMarshallableCfg implements ClientConnectionMonitor {

    @Override
    public void onConnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onConnected name=" + name + ",socketAddress=" + socketAddress);
    }

    @Override
    public void onDisconnected(String name, @Nullable SocketAddress socketAddress) {
        debug().on(this.getClass(), "onDisconnected name=" + name + ",socketAddress=" + socketAddress);
    }

}
