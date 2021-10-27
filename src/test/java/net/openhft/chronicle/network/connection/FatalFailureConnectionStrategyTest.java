/*
 * Copyright 2016-2020 Chronicle Software
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

import net.openhft.chronicle.wire.Marshallable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that {@link FatalFailureConnectionStrategy} can be used in YAML config.
 */
public class FatalFailureConnectionStrategyTest {
    @Test
    public void testFromYaml() {
        final FatalFailureConnectionStrategy strategy = new FatalFailureConnectionStrategy(3, false);

        final FatalFailureConnectionStrategy strategyFromYaml = Marshallable.fromString(
                "!net.openhft.chronicle.network.connection.FatalFailureConnectionStrategy {\n" +
                        "  attempts: 3,\n" +
                        "  blocking: false\n" +
                        "}");

        Assert.assertNotNull(strategyFromYaml);

        final String expectedToString = "!net.openhft.chronicle.network.connection.FatalFailureConnectionStrategy {\n" +
                "  attempts: 3,\n" +
                "  blocking: false,\n" +
                "  tcpBufferSize: " + TcpChannelHub.TCP_BUFFER + ",\n" +
                "  clientConnectionMonitor: !net.openhft.chronicle.network.VanillaClientConnectionMonitor {\n" +
                "  }\n" +
                "}\n";

        Assert.assertEquals(expectedToString, strategy.toString());
        Assert.assertEquals(strategy.toString(), strategyFromYaml.toString());

        Assert.assertFalse(strategy.isClosed());
        Assert.assertFalse(strategyFromYaml.isClosed());

        strategy.close();
        strategyFromYaml.close();

        Assert.assertTrue(strategy.isClosed());
        Assert.assertTrue(strategyFromYaml.isClosed());
    }
}
