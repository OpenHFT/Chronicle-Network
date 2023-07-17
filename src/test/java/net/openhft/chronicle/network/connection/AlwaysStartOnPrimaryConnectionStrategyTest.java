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
import net.openhft.chronicle.network.AlwaysStartOnPrimaryConnectionStrategy;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.util.TestServer;
import net.openhft.chronicle.wire.Marshallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;

import static net.openhft.chronicle.network.NetworkUtil.TCP_BUFFER_SIZE;
import static net.openhft.chronicle.network.util.TestUtil.getAvailablePortNumber;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class AlwaysStartOnPrimaryConnectionStrategyTest {

    @Test
    @Timeout(10)
    void testLocalBinding() throws InterruptedException, IOException {
        try (AlwaysStartOnPrimaryConnectionStrategy strategy = new AlwaysStartOnPrimaryConnectionStrategy()) {
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
}
