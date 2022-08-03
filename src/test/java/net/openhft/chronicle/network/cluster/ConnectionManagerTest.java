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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConnectionManagerTest extends NetworkTestCommon {

    private ConnectionManager<TestNetworkContext> connectionManager;
    private TestNetworkContext networkContext;

    private ConnectionManager.ConnectionListener<TestNetworkContext> listener1;
    private ConnectionManager.ConnectionListener<TestNetworkContext> listener2;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        listener1 = Mockito.mock(ConnectionManager.ConnectionListener.class);
        listener2 = Mockito.mock(ConnectionManager.ConnectionListener.class);
        connectionManager = new ConnectionManager<>();
        networkContext = new TestNetworkContext();
    }

    @AfterEach
    void closeNetworkContext() {
        Closeable.closeQuietly(networkContext);
    }

    @Test
    void onConnectionChangedExecutesAllListeners() {
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.onConnectionChanged(true, networkContext, null);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    void onConnectionChangedOnlyExecutesListenersWhenConnectionStateChanges() {
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
        connectionManager.onConnectionChanged(true, networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
        connectionManager.onConnectionChanged(false, networkContext, token);
        verify(listener1).onConnectionChange(networkContext, false);
        verify(listener2).onConnectionChange(networkContext, false);
        verifyNoMoreInteractions(listener1, listener2);
    }

    @Test
    void executeNewListenersWillOnlyExecuteNonExecutedListeners() {
        connectionManager.addListener(listener1);
        ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.onConnectionChanged(true, networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
        connectionManager.addListener(listener2);
        connectionManager.executeNewListeners(networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
        connectionManager.executeNewListeners(networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
        verifyNoMoreInteractions(listener1, listener2);
    }

    @Test
    void executeNewListenersWillExecuteAllListenersOnFirstCall() {
        final ConnectionManager.EventEmitterToken eventEmitterToken = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.executeNewListeners(networkContext, eventEmitterToken);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    void executeNewListenersWillNotExecuteListenersThatPreviouslyThrewIllegalStateException() {
        doThrow(IllegalStateException.class).when(listener2).onConnectionChange(any(), anyBoolean());
        final ConnectionManager.EventEmitterToken eventEmitterToken = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.executeNewListeners(networkContext, eventEmitterToken);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    void onConnectionChangedWorksWhenThereAreNoListeners() {
        final ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.onConnectionChanged(false, networkContext, token);
        verify(listener1).onConnectionChange(networkContext, false);
    }

    @Test
    void executeNewListenersWorksWhenThereAreNoListeners() {
        final ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.executeNewListeners(networkContext, token);
        connectionManager.addListener(listener1);
        connectionManager.executeNewListeners(networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
    }

    static class TestNetworkContext extends VanillaNetworkContext<TestNetworkContext> {
    }
}