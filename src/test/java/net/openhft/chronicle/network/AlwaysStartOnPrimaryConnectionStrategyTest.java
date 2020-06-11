package net.openhft.chronicle.network;

import net.openhft.chronicle.network.connection.FatalFailureMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class AlwaysStartOnPrimaryConnectionStrategyTest extends NetworkTestCommon {
    private static String uri;

    @After
    public void teardown() {
        TCPRegistry.reset();
    }

    @Before
    public void setUp() throws IOException {
        String hostPort = "host.port";
        TCPRegistry.createServerSocketChannelFor(hostPort);
        uri = TCPRegistry.acquireServerSocketChannel(hostPort).getLocalAddress().toString();
    }

    @Test(timeout = 1_000)
    public void connect_attempts_should_stop_when_thread_is_interrupted() throws InterruptedException {
        Thread thread = new Thread(() -> {
            ConnectionStrategy strategy = new AlwaysStartOnPrimaryConnectionStrategy();
            try {
                strategy.connect("unavailable_uri", SocketAddressSupplier.uri(uri), false, new FatalFailureMonitor() {});
            } catch (InterruptedException e) {
                Assert.fail("AlwaysStartOnPrimaryConnectionStrategy#connect should not have propagated the " + e.getClass());
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
    }
}
