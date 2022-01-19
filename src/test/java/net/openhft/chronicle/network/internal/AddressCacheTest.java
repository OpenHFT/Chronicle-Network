package net.openhft.chronicle.network.internal;

import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.net.InetSocketAddress;
import java.security.Security;

import static org.junit.Assert.*;

public class AddressCacheTest {

    private static final String NETWORK_ADDRESS_CACHE_TTL = "networkaddress.cache.ttl";
    private static final String DEFAULT_NETWORK_ADDRESS_CACHE_TTL = "-1";
    private AddressCache addressCache;

    @Before
    public void setUp() {
        addressCache = new AddressCache();
    }

    @Test
    public void addingEntryWillMakeItAvailableForLookup() {
        addressCache.add("test-123", "google.com", 123);
        assertEquals(addressCache.lookup("test-123"), new InetSocketAddress("google.com", 123));
    }

    @Test
    public void entriesAreUnavailableUntilAdded() {
        assertNull(addressCache.lookup("test-123"));
    }

    @Test
    public void clearingWillMakeItUnavailableForLookup() {
        addressCache.add("test-123", "google.com", 123);
        assertEquals(addressCache.lookup("test-123"), new InetSocketAddress("google.com", 123));
        addressCache.clear();
        assertNull(addressCache.lookup("test-123"));
    }

    @Test
    public void resolvedAddressesAreCachedWhenCachingIsEnabled() throws Throwable {
        withNetworkAddressCacheTtl("100", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertSame(original, addressCache.lookup("test-123"));
        });
    }

    @Test
    public void resolvedAddressesAreCachedWhenCachingForeverIsEnabled() throws Throwable {
        withNetworkAddressCacheTtl("-1", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertSame(original, addressCache.lookup("test-123"));
        });
    }

    @Test
    public void resolvedAddressesAreNotCachedWhenCachingIsDisabled() throws Throwable {
        withNetworkAddressCacheTtl("0", () -> {
            addressCache.add("test-123", "google.com", 123);
            InetSocketAddress original = addressCache.lookup("test-123");
            assertNotSame(original, addressCache.lookup("test-123"));
        });
    }

    private void withNetworkAddressCacheTtl(String ttlValue, ThrowingRunnable testCode) throws Throwable {
        Security.setProperty(NETWORK_ADDRESS_CACHE_TTL, ttlValue);
        try {
            testCode.run();
        } finally {
            Security.setProperty(NETWORK_ADDRESS_CACHE_TTL, DEFAULT_NETWORK_ADDRESS_CACHE_TTL);
        }
    }
}