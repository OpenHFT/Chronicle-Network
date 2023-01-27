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

import net.openhft.chronicle.network.internal.lookuptable.FileBasedHostnamePortLookupTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

class CrossProcessTCPRegistryTest extends TCPRegistryTest {

    @BeforeEach
    void setUp() {
        TCPRegistry.useCrossProcessRegistry();
    }

    @Test
    void resetOnFreshRegistryWillClearExistingEntries() throws IOException {
        final String hostAlias = "should_be_cleared";
        try (final FileBasedHostnamePortLookupTable lookupTable = new FileBasedHostnamePortLookupTable()) {
            // write an alias to the lookup table file
            lookupTable.put(hostAlias, new InetSocketAddress(0));
            // reset before static lookup table is lazily created
            TCPRegistry.reset();
            // this should not log any warnings, the alias should be clear
            TCPRegistry.createServerSocketChannelFor(hostAlias);
        }
    }

    @AfterEach
    void tearDown() {
        TCPRegistry.useInMemoryRegistry();
    }
}
