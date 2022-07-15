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