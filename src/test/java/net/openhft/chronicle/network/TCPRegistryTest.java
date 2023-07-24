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
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;

public abstract class TCPRegistryTest extends NetworkTestCommon {

    /**
     * {@link TCPRegistry#lookup(String)} contains logic to look up an address from system properties where the key
     * maps to a host:port pair. This key constant is used to map to various host:port combinations in tests below.
     */
    public static final String SYSTEM_PROPERTY_HOST_DETAILS_KEY = "exampleHostKey";

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

    @Test
    void testAllServersStopped() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        TCPRegistry.reset();
        TCPRegistry.assertAllServersStopped();
    }

    @Test
    void createServerSocketChannelFor_withHostAndInvalidPort() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TCPRegistry.createServerSocketChannelFor("host1:-1")
        );
        assertEquals("port out of range:-1", exception.getMessage());
    }

    @Test
    public void lookup_lookupViaSystemProperty_empty() {
        try {
            System.setProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY, "");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TCPRegistry.lookup(SYSTEM_PROPERTY_HOST_DETAILS_KEY));
            assertEquals("Alias " + SYSTEM_PROPERTY_HOST_DETAILS_KEY + " as  malformed, expected hostname:port", exception.getMessage());
        } finally {
            System.clearProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY);
        }
    }

    @Test
    public void lookup_lookupViaSystemProperty_nullHostname() {
        try {
            System.setProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY, "null:");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TCPRegistry.lookup(SYSTEM_PROPERTY_HOST_DETAILS_KEY));
            assertEquals("Invalid hostname \"null\"", exception.getMessage());
        } finally {
            System.clearProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY);
        }
    }

    @Test
    public void lookup_lookupViaSystemProperty_invalidPort() {
        try {
            System.setProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY, "a:z");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TCPRegistry.lookup(SYSTEM_PROPERTY_HOST_DETAILS_KEY));
            assertEquals("Alias " + SYSTEM_PROPERTY_HOST_DETAILS_KEY + " as a:z malformed, expected hostname:port with port as a number", exception.getMessage());
        } finally {
            System.clearProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY);
        }
    }

    @Test
    public void lookup_lookupViaSystemProperty_wellFormed() {
        try {
            System.setProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY, "host:9999");
            InetSocketAddress address = TCPRegistry.lookup(SYSTEM_PROPERTY_HOST_DETAILS_KEY);
            assertEquals("host", address.getHostName());
            assertEquals(9999, address.getPort());
        } finally {
            System.clearProperty(SYSTEM_PROPERTY_HOST_DETAILS_KEY);
        }
    }

    @Test
    public void lookup_fallback_wellFormed() {
        InetSocketAddress address = TCPRegistry.lookup("host:9999");
        assertEquals("host", address.getHostName());
        assertEquals(9999, address.getPort());
    }

    @Test
    public void lookup_fallback_malformedPort() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TCPRegistry.lookup("host:a"));
        assertEquals("Description host:a malformed, expected hostname:port with port as a number", exception.getMessage());
    }

    @Test
    void dumpAll() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        TCPRegistry.dumpAllSocketChannels();
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