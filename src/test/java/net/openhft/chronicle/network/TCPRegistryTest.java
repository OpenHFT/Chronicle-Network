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


import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class TCPRegistryTest extends NetworkTestCommon {

    @Test
    void testResetClearsRegistry() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        assertNotNull(TCPRegistry.lookup("host1"));
        assertNotNull(TCPRegistry.lookup("host2"));
        assertNotNull(TCPRegistry.lookup("host3"));
        TCPRegistry.reset();
        assertNotMapped("host1");
        assertNotMapped("host2");
        assertNotMapped("host3");
    }

    @Test
    void testResetIsIdempotent() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        TCPRegistry.reset();
        TCPRegistry.reset();
    }

    private void assertNotMapped(String hostName) {
        try {
            TCPRegistry.lookup(hostName);
            fail(String.format("Found mapping for %s", hostName));
        } catch (IllegalArgumentException ex) {
            // This is good
        }
    }
}