package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionManagerTest extends NetworkTestCommon {

    private ConnectionManager<TestNetworkContext> connectionManager;
    private TestNetworkContext networkContext;

    @Mock
    private ConnectionManager.ConnectionListener<TestNetworkContext> listener1;
    @Mock
    private ConnectionManager.ConnectionListener<TestNetworkContext> listener2;

    @Before
    public void setUp() {
        connectionManager = new ConnectionManager<>();
        networkContext = new TestNetworkContext();
    }

    @After
    public void tearDown() {
        Closeable.closeQuietly(networkContext);
    }

    @Test
    public void onConnectionChangedExecutesAllListeners() {
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.onConnectionChanged(true, networkContext, null);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    public void onConnectionChangedOnlyExecutesListenersWhenConnectionStateChanges() {
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
    public void executeNewListenersWillOnlyExecuteNonExecutedListeners() {
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
    public void executeNewListenersWillExecuteAllListenersOnFirstCall() {
        final ConnectionManager.EventEmitterToken eventEmitterToken = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.executeNewListeners(networkContext, eventEmitterToken);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    public void executeNewListenersWillNotExecuteListenersThatPreviouslyThrewIllegalStateException() {
        doThrow(IllegalStateException.class).when(listener2).onConnectionChange(any(), anyBoolean());
        final ConnectionManager.EventEmitterToken eventEmitterToken = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.addListener(listener2);
        connectionManager.executeNewListeners(networkContext, eventEmitterToken);
        verify(listener1).onConnectionChange(networkContext, true);
        verify(listener2).onConnectionChange(networkContext, true);
    }

    @Test
    public void onConnectionChangedWorksWhenThereAreNoListeners() {
        final ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.addListener(listener1);
        connectionManager.onConnectionChanged(false, networkContext, token);
        verify(listener1).onConnectionChange(networkContext, false);
    }

    @Test
    public void executeNewListenersWorksWhenThereAreNoListeners() {
        final ConnectionManager.EventEmitterToken token = connectionManager.onConnectionChanged(true, networkContext, null);
        connectionManager.executeNewListeners(networkContext, token);
        connectionManager.addListener(listener1);
        connectionManager.executeNewListeners(networkContext, token);
        verify(listener1).onConnectionChange(networkContext, true);
    }

    static class TestNetworkContext extends VanillaNetworkContext<TestNetworkContext> {
    }
}