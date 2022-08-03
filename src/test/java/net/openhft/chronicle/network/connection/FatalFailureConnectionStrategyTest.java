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

import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.Marshallable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checks that {@link FatalFailureConnectionStrategy} can be used in YAML config.
 */
class FatalFailureConnectionStrategyTest {
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
                "  attempts: 3,\n" +
                "  tcpBufferSize: " + TcpChannelHub.TCP_BUFFER + ",\n" +
                "  clientConnectionMonitor: !VanillaClientConnectionMonitor {\n" +
                "  }\n" +
                "}\n";

        assertEquals(expectedToString, strategy.toString());
        assertEquals(strategy.toString(), strategyFromYaml.toString());

        assertFalse(strategy.isClosed());
        assertFalse(strategyFromYaml.isClosed());

        strategy.close();
        strategyFromYaml.close();

        assertTrue(strategy.isClosed());
        assertTrue(strategyFromYaml.isClosed());
    }
}
