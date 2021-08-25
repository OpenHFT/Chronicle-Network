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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.ConnectionListener;

public enum ConnectionListeners implements ConnectionListener {
    NONE {
        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            // Do Nothing
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            // Do Nothing
        }
    },
    LOGGING {
        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.startup().on(getClass(), "onConnected lid:" + localIdentifier + ", rid:" + remoteIdentifier + ", acceptor:" + isAcceptor);
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.startup().on(getClass(), "onDisconnected lid:" + localIdentifier + ", rid:" + remoteIdentifier + ", acceptor:" + isAcceptor);
        }
    }
}
