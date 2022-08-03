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

import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.network.TCPRegistry.createInetSocketAddress;

/**
 * A cache of {@link InetSocketAddress}es.
 * <p>
 * This will prevent the creation of a new {@link InetSocketAddress} every time, but will configure expiry
 * consistent with the JVM <em>networkaddress.cache.ttl</em> setting.
 * <p>
 * e.g. if <em>networkaddress.cache.ttl</em> is set to:
 * <dl>
 *   <dt>0</dt>
 *      <dd>caching will be disabled altogether, this cache will just remember e.g. description to system property mappings</dd>
 *   <dt>30</dt>
 *      <dd>InetSocketAddresses will be cached for 30s after their first lookup</dd>
 *   <dt>any negative value</dt>
 *      <dd>InetSocketAddresses will be cached forever</dd>
 * </dl>
 * <p>
 * Note: This will default to caching addresses forever when no value is specified for <em>networkaddress.cache.ttl</em>,
 * as per the documentation, that default may differ from the JVM default when no security manager is installed.
 */
public class AddressCache {

    private static final String NETWORK_ADDRESS_CACHE_TTL = "networkaddress.cache.ttl";
    private static final int NEVER_EXPIRE = -1;

    private final Map<String, CacheEntry> cachedAddresses;

    public AddressCache() {
        cachedAddresses = new ConcurrentHashMap<>();
    }

    public InetSocketAddress lookup(String description) {
        final CacheEntry cacheEntry = cachedAddresses.get(description);
        return cacheEntry != null ? cacheEntry.address() : null;
    }

    public void add(String description, String hostname, int port) {
        cachedAddresses.put(description, new CacheEntry(hostname, port));
    }

    public void clear() {
        cachedAddresses.clear();
    }

    private static final class CacheEntry {
        @NotNull
        private final String hostname;
        private final int port;
        private InetSocketAddress inetSocketAddress;
        private long expiryTime;

        public CacheEntry(@NotNull String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        /**
         * Get the {@link InetSocketAddress} for this entry, creating a new one if necessary
         * <p>
         * Note: it is important this method stays synchronized, to prevent unnecessary InetSocketAddress creation
         * but also to ensure any updates to the fields are visible to all threads.
         *
         * @return The {@link InetSocketAddress}
         */
        public synchronized InetSocketAddress address() {
            if (isCachingEnabled()) {
                if (inetSocketAddress == null || (expiryTime >= 0 && System.currentTimeMillis() > expiryTime)) {
                    inetSocketAddress = createInetSocketAddress(hostname, port);
                    expiryTime = calculateExpiryTime();
                }
                return inetSocketAddress;
            } else {
                return createInetSocketAddress(hostname, port);
            }
        }

        /**
         * Calculate the expiry time for the cache entry using <em>networkaddress.cache.ttl</em>
         * <p>
         * See https://docs.oracle.com/javase/7/docs/technotes/guides/net/properties.html for details
         *
         * @return The expiry time in milliseconds since the epoch or -1 to never expire
         */
        private long calculateExpiryTime() {
            final String cacheTTL = Security.getProperty(NETWORK_ADDRESS_CACHE_TTL);
            if (cacheTTL != null) {
                final long parsedCacheTTL = Long.parseLong(cacheTTL);
                return parsedCacheTTL < 0 ? NEVER_EXPIRE : System.currentTimeMillis() + (1_000 * parsedCacheTTL);
            } else {
                return NEVER_EXPIRE;
            }
        }

        /**
         * Is INetSocketAddress caching enabled?
         *
         * @return true if caching is enabled, false otherwise
         */
        private boolean isCachingEnabled() {
            final String cacheTTL = Security.getProperty(NETWORK_ADDRESS_CACHE_TTL);
            return cacheTTL == null || Long.parseLong(cacheTTL) != 0;
        }
    }
}
