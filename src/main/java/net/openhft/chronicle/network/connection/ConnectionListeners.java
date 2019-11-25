/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.ConnectionListener;

public enum ConnectionListeners implements ConnectionListener {
    NONE {
        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
        }
    },
    LOGGING {
        @Override
        public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.warn().on(getClass(), "onConnected lid:" + localIdentifier + ", rid:" + remoteIdentifier + ", acceptor:" + isAcceptor);
        }

        @Override
        public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            Jvm.warn().on(getClass(), "onDisconnected lid:" + localIdentifier + ", rid:" + remoteIdentifier + ", acceptor:" + isAcceptor);
        }
    }
}
