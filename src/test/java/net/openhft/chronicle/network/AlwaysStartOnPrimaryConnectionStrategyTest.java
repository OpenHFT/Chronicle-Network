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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.connection.FatalFailureMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.util.TestServer;
import net.openhft.chronicle.wire.JSONWire;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import static net.openhft.chronicle.network.util.TestUtil.getAvailablePortNumber;
import static org.junit.jupiter.api.Assertions.*;

class AlwaysStartOnPrimaryConnectionStrategyTest extends NetworkTestCommon {
    private static String uri;

    @BeforeEach
    void setUp() throws IOException {
        String hostPort = "host.port";
        TCPRegistry.createServerSocketChannelFor(hostPort);
        uri = TCPRegistry.acquireServerSocketChannel(hostPort).getLocalAddress().toString();
    }

    @Test
    @Timeout(1)
    void connectAttemptsShouldStopWhenThreadIsInterrupted() throws InterruptedException {
        Thread thread = new Thread(() -> {
            ConnectionStrategy strategy = new AlwaysStartOnPrimaryConnectionStrategy();
            try {
                strategy.connect("unavailable_uri", SocketAddressSupplier.uri(uri), false, FatalFailureMonitor.NO_OP);
            } catch (InterruptedException e) {
                fail("AlwaysStartOnPrimaryConnectionStrategy#connect should not have propagated the " + e.getClass());
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
    }

    @Test
    void testIsSerializable() {
        TCPRegistry.reset();
        final AlwaysStartOnPrimaryConnectionStrategy alwaysStartOnPrimaryConnectionStrategy = new AlwaysStartOnPrimaryConnectionStrategy();
        JSONWire jsonWire = new JSONWire().useTypes(true);
        jsonWire.getValueOut().object(alwaysStartOnPrimaryConnectionStrategy);
        assertEquals("{\"@AlwaysStartOnPrimaryConnectionStrategy\":{}}", jsonWire.bytes().toString());
    }

    @Test
    void testLocalBinding() throws InterruptedException, IOException {
        final AlwaysStartOnPrimaryConnectionStrategy strategy = new AlwaysStartOnPrimaryConnectionStrategy();
        final String localSocketBindingHost = "127.0.0.75";
        int localPort = getAvailablePortNumber();
        strategy.localSocketBindingHost(localSocketBindingHost);
        strategy.localSocketBindingPort(localPort);
        try (TestServer testServer = new TestServer("localBindingTestServer")) {
            testServer.prepareToAcceptAConnection();
            Jvm.pause(100);
            try (final ChronicleSocketChannel channel = strategy.connect("local_server", SocketAddressSupplier.uri(testServer.uri()), false, null)) {
                assertNotNull(channel);
                final InetSocketAddress localSocketAddress = (InetSocketAddress) channel.socket().getLocalSocketAddress();
                assertEquals(localPort, localSocketAddress.getPort());
                assertEquals(localSocketBindingHost, localSocketAddress.getHostName());
            }
        }
    }

}
