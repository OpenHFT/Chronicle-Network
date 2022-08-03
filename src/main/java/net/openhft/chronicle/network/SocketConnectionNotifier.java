/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.Marshallable;

import static net.openhft.chronicle.core.Mocker.intercepting;

@FunctionalInterface
public interface SocketConnectionNotifier<T extends NetworkContext> extends Marshallable {

    static SocketConnectionNotifier newDefaultConnectionNotifier() {
        return intercepting(SocketConnectionNotifier.class,
                "connection ",
                msg -> Jvm.debug().on(SocketConnectionNotifier.class, msg));
    }

    void onConnected(String host, long port, T nc);

    default void onDisconnected(String host, long port) {
        // Do nothing
    }
}
