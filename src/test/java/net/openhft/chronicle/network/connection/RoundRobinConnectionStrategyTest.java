package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RoundRobinConnectionStrategyTest {

    private static final int PAUSE_PERIOD_MS = 111;
    private static final int TCP_BUFFER_SIZE = 222;
    private static final int SOCKET_CONNECTION_TIMEOUT_MS = 333;
    private RoundRobinConnectionStrategy connectionStrategy;

    @Mock
    private OpenSocketStrategy openSocketStrategy;
    @Mock
    private FatalFailureMonitor fatalFailureMonitor;
    @Mock
    private ChronicleSocketChannel chronicleSocketChannel;

    @BeforeEach
    void setUp() {
        connectionStrategy = new RoundRobinConnectionStrategy(openSocketStrategy)
                .pausePeriodMs(PAUSE_PERIOD_MS)
                .tcpBufferSize(TCP_BUFFER_SIZE)
                .socketConnectionTimeoutMs(SOCKET_CONNECTION_TIMEOUT_MS);
    }

    @Test
    void willReturnSocketWhenConnectionEstablished() throws InterruptedException, IOException {
        SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(new String[]{"127.0.0.1:123", "127.0.0.2:123", "127.0.0.3:123"}, "sas");
        when(openSocketStrategy.openSocketChannel(same(connectionStrategy), any(InetSocketAddress.class), anyInt(), anyLong(), anyInt())).thenReturn(chronicleSocketChannel);
        assertSame(chronicleSocketChannel, connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
        verifyNoInteractions(fatalFailureMonitor);
    }

    @Test
    void willReturnNullAfterTryingAllAddresses() throws InterruptedException, IOException {
        SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(new String[]{"127.0.0.1:123", "127.0.0.2:123", "127.0.0.3:123"}, "sas");
        when(openSocketStrategy.openSocketChannel(same(connectionStrategy), any(InetSocketAddress.class), anyInt(), anyLong(), anyInt())).thenReturn(null);
        assertNull(connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.2:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.3:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
        verify(fatalFailureMonitor).onFatalFailure("test",
                "Failed to connect to any of these servers=[/127.0.0.1:123 - 127.0.0.1:123, /127.0.0.2:123 - 127.0.0.2:123, /127.0.0.3:123 - 127.0.0.3:123]");
    }

    @Test
    void willResumeFromNextAddressWhenCalledAgainAfterSuccess() throws IOException, InterruptedException {
        SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(new String[]{"127.0.0.1:123", "127.0.0.2:123", "127.0.0.3:123"}, "sas");
        when(openSocketStrategy.openSocketChannel(same(connectionStrategy), any(InetSocketAddress.class), anyInt(), anyLong(), anyInt())).thenReturn(null);
        when(openSocketStrategy.openSocketChannel(same(connectionStrategy), eq(TCPRegistry.lookup("127.0.0.2:123")), anyInt(), anyLong(), anyInt())).thenReturn(chronicleSocketChannel);
        assertSame(chronicleSocketChannel, connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.2:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
        clearInvocations(openSocketStrategy);
        assertSame(chronicleSocketChannel, connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.3:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.2:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
    }

    @Test
    void willResumeFromNextAddressWhenCalledAgainAfterFailure() throws IOException, InterruptedException {
        SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(new String[]{"127.0.0.1:123", "127.0.0.2:123", "127.0.0.3:123"}, "sas");
        when(openSocketStrategy.openSocketChannel(same(connectionStrategy), any(InetSocketAddress.class), anyInt(), anyLong(), anyInt())).thenReturn(null);
        assertNull(connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.2:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.3:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
        clearInvocations(openSocketStrategy);
        assertNull(connectionStrategy.connect("test", socketAddressSupplier, ConnectionState.UNKNOWN, fatalFailureMonitor));
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.1:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.2:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verify(openSocketStrategy).openSocketChannel(connectionStrategy, TCPRegistry.lookup("127.0.0.3:123"), TCP_BUFFER_SIZE, PAUSE_PERIOD_MS, SOCKET_CONNECTION_TIMEOUT_MS);
        verifyNoMoreInteractions(openSocketStrategy);
    }
}