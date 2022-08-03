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

package net.openhft.chronicle.network.internal;

import net.openhft.chronicle.core.util.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.*;

class AddressCacheTest {

    private static final String NETWORK_ADDRESS_CACHE_TTL = "networkaddress.cache.ttl";
    private static final String DEFAULT_NETWORK_ADDRESS_CACHE_TTL = "-1";
    private AddressCache addressCache;

    @BeforeEach
    void setUp() {
        addressCache = new AddressCache();
    }

    @Test
    void addingEntryWillMakeItAvailableForLookup() {
        addressCache.add("test-123", "google.com", 123);
        assertEquals(addressCache.lookup("test-123"), new InetSocketAddress("google.com", 123));
    }

    @Test
    void entriesAreUnavailableUntilAdded() {
        assertNull(addressCache.lookup("test-123"));
    }

    @Test
    void clearingWillMakeItUnavailableForLookup() {
        addressCache.add("test-123", "google.com", 123);
        assertEquals(addressCache.lookup("test-123"), new InetSocketAddress("google.com", 123));
        addressCache.clear();
        assertNull(addressCache.lookup("test-123"));
    }

    @Test
    void resolvedAddressesAreCachedWhenCachingIsEnabled() throws Throwable {
        withNetworkAddressCacheTtl("100", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertSame(original, addressCache.lookup("test-123"));
        });
    }

    @Test
    void resolvedAddressesAreCachedWhenCachingForeverIsEnabled() throws Throwable {
        withNetworkAddressCacheTtl("-1", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertSame(original, addressCache.lookup("test-123"));
        });
    }

    @Test
    void resolvedAddressesAreNotCachedWhenCachingIsDisabled() throws Throwable {
        withNetworkAddressCacheTtl("0", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertNotSame(original, addressCache.lookup("test-123"));
        });
    }

    private void withNetworkAddressCacheTtl(String ttlValue, ThrowingRunnable<?> testCode) throws Throwable {
        Security.setProperty(NETWORK_ADDRESS_CACHE_TTL, ttlValue);
        try {
            testCode.run();
        } finally {
            Security.setProperty(NETWORK_ADDRESS_CACHE_TTL, DEFAULT_NETWORK_ADDRESS_CACHE_TTL);
        }
    }
}