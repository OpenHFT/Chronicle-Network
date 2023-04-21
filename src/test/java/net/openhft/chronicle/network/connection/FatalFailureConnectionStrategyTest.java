/*
 * Copyright 2016-2020 Chronicle Software
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
package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.util.TestServer;
import net.openhft.chronicle.wire.Marshallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;

import static net.openhft.chronicle.network.util.TestUtil.getAvailablePortNumber;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class FatalFailureConnectionStrategyTest {

    /**
     * Checks that {@link FatalFailureConnectionStrategy} can be used in YAML config.
     */
    @Test
    void testFromYaml() {
        TCPRegistry.reset();
        final FatalFailureConnectionStrategy strategy = new FatalFailureConnectionStrategy(3, false);

        final FatalFailureConnectionStrategy strategyFromYaml = Marshallable.fromString(
                "!FatalFailureConnectionStrategy {\n" +
                        "  attempts: 3,\n" +
                        "  blocking: false\n" +
                        "}");

        assertNotNull(strategyFromYaml);

        final String expectedToString = "!FatalFailureConnectionStrategy {\n" +
                "  attempts: 3\n" +
                "}\n";

        assertEquals(expectedToString, strategy.toString());
        assertEquals(strategy.toString(), strategyFromYaml.toString());

        assertEquals(TcpChannelHub.TCP_BUFFER, strategy.tcpBufferSize);
        assertEquals(TcpChannelHub.TCP_BUFFER, strategyFromYaml.tcpBufferSize);
        assertNotNull(strategy.clientConnectionMonitor);
        assertNotNull(strategyFromYaml.clientConnectionMonitor);

        assertFalse(strategy.isClosed());
        assertFalse(strategyFromYaml.isClosed());

        strategy.close();
        strategyFromYaml.close();

        assertTrue(strategy.isClosed());
        assertTrue(strategyFromYaml.isClosed());
    }

    @Test
    @Timeout(10)
    void testLocalBinding() throws InterruptedException, IOException {
        assumeFalse(OS.isMacOSX()); // doesn't work on mac?
        final FatalFailureConnectionStrategy strategy = new FatalFailureConnectionStrategy(1, true);
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
