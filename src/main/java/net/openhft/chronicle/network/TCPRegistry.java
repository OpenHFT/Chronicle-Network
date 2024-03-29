/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.network.api.NetworkStats;
import net.openhft.chronicle.network.connection.FatalFailureConnectionStrategy;
import net.openhft.chronicle.network.internal.AddressCache;
import net.openhft.chronicle.network.internal.lookuptable.FileBasedHostnamePortLookupTable;
import net.openhft.chronicle.network.internal.lookuptable.ProcessLocalHostnamePortLookupTable;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketFactory;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory.wrap;

/**
 * The TCPRegistry allows you to either provide a true host and port for example "localhost:8080" or if you would rather let the application allocate
 * you a free port at random, you can just provide a text reference to the port, for example "host.port", you can provide any text you want, it will
 * always be taken as a reference, that is unless its correctly formed like "&lt;hostname&gt;:&lt;port&gt;”, then it will use the exact host and port
 * you provide. For listen addresses, if hostname is empty or * then all local addresses will be listened on. <p>The reason we offer this
 * functionality is quite often in unit tests you wish to start a test via loopback, followed often by another test via loopback, if the first test
 * does not shut down correctly it can impact on the second test. Giving each test a unique port is one solution, but then managing those ports can
 * become a problem in its self. So we created the TCPRegistry which manages those ports for you, when you come to clean up at the end of each test,
 * all you have to do it call TCPRegistry.reset() and it will ensure that any remaining ports that are open will be closed.
 */
public enum TCPRegistry {
    ;
    public static final String TCP_REGISTRY_LOOKUP_TABLE_IMPLEMENTATION_PROPERTY = "chronicle.tcpRegistry.lookupTableImplementation";
    static HostnamePortLookupTable lookupTable;
    static final Map<String, ChronicleServerSocketChannel> DESC_TO_SERVER_SOCKET_CHANNEL_MAP = new ConcurrentSkipListMap<>();
    private static final AddressCache ADDRESS_CACHE = new AddressCache();

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(
                VanillaClientConnectionMonitor.class,
                FatalFailureConnectionStrategy.class,
                NetworkStats.class,
                AlwaysStartOnPrimaryConnectionStrategy.class);
    }

    /**
     * Configure the system property to use the cross-process lookup table, this won't come into effect until the next reset
     */
    public static void useCrossProcessRegistry() {
        System.setProperty(TCP_REGISTRY_LOOKUP_TABLE_IMPLEMENTATION_PROPERTY,
                FileBasedHostnamePortLookupTable.class.getName());
    }

    /**
     * Configure the system property to use the in-memory lookup table, this won't come into effect until the next reset
     */
    public static void useInMemoryRegistry() {
        System.setProperty(TCP_REGISTRY_LOOKUP_TABLE_IMPLEMENTATION_PROPERTY,
                ProcessLocalHostnamePortLookupTable.class.getName());
    }

    public static void reset() {
        lookupTable();  // initialise the lookup table so persistent tables are cleared
        Closeable.closeQuietly(DESC_TO_SERVER_SOCKET_CHANNEL_MAP.values());
        Closeable.closeQuietly(lookupTable);
        lookupTable = null;
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.clear();
        ADDRESS_CACHE.clear();
        Jvm.pause(50);
    }

    public static Set<String> aliases() {
        return lookupTable().aliases();
    }

    public static void assertAllServersStopped() {
        @NotNull List<String> closed = new ArrayList<>();
        for (@NotNull Map.Entry<String, ChronicleServerSocketChannel> entry : DESC_TO_SERVER_SOCKET_CHANNEL_MAP.entrySet()) {
            if (entry.getValue().isOpen())
                closed.add(entry.toString());
            closeQuietly(entry.getValue());
        }
        lookupTable().clear();
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.clear();
        if (!closed.isEmpty())
            throw new AssertionError("Had to stop " + closed);
    }

    public static void setAlias(String name, @NotNull String hostname, int port) {
        addInetSocketAddress(name, hostname, port);
    }

    /**
     * Creates and returns a new ChronicleServerSocketChannel.
     *
     * @param descriptions each string is the name to a reference of a host and port, or if correctly formed this example host and port are used
     *                     instead
     * @return a new ChronicleServerSocketChannel
     * @throws IOException if the socket cannot be created
     */
    public static ChronicleServerSocketChannel createServerSocketChannelFor(@NotNull String descriptions) throws IOException {
        return createServerSocketChannelFor(false, descriptions);
    }

    public static void createServerSocketChannelFor(@NotNull String... descriptions) throws IOException {
        createServerSocketChannelFor(false, descriptions);
    }

    public static ChronicleServerSocketChannel createServerSocketChannelFor(boolean isNative, @NotNull String... descriptions) throws IOException {
        for (@NotNull String description : descriptions) {
            InetSocketAddress address;
            if (description.contains(":")) {
                @NotNull String[] split = description.trim().split(":");
                String host = split[0];
                int port = Integer.parseInt(split[1]);
                address = createInetSocketAddress(host, port);
            } else {
                address = new InetSocketAddress("localhost", 0);
            }
            ChronicleServerSocketChannel ssc = createSSC(isNative, description, address);
            if (descriptions.length == 1)
                return ssc;
        }
        return null;
    }

    private static ChronicleServerSocketChannel createSSC(boolean isNative, String description, InetSocketAddress address) throws IOException {
        ChronicleServerSocketChannel ssc = isNative ? ChronicleServerSocketFactory.openNative() : ChronicleServerSocketFactory.open(description);
        ssc.socket().setReuseAddress(true);
        ssc.bind(address);
        assert ssc.isOpen();
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.put(description, ssc);
        lookupTable().put(description, (InetSocketAddress) ssc.socket().getLocalSocketAddress());
        return ssc;
    }

    public static ChronicleServerSocketChannel acquireServerSocketChannel(@NotNull String description) throws IOException {
        ChronicleServerSocketChannel ssc = DESC_TO_SERVER_SOCKET_CHANNEL_MAP.get(description);
        if (ssc != null && ssc.isOpen())
            return ssc;
        InetSocketAddress address = lookup(description);
        ssc = ChronicleServerSocketFactory.open(description);

        // PLEASE DON'T CHANGE THIS TO FALSE AS IT CAUSES RESTART ISSUES ON TCP/IP TIMED_WAIT
        ssc.socket().setReuseAddress(true);

        try {
            ssc.bind(address);
        } catch (Exception e) {
            Jvm.warn().on(TCPRegistry.class, "Error when attempting to bind to address " + address, e);
            throw Jvm.rethrow(e);
        }

        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.put(description, ssc);
        return ssc;
    }

    public static InetSocketAddress lookup(@NotNull String description) {
        InetSocketAddress address = lookupTable().lookup(description);
        if (address != null)
            return address;
        address = ADDRESS_CACHE.lookup(description);
        if (address != null)
            return address;
        String property = Jvm.getProperty(description);
        if (property != null) {
            @NotNull String[] parts = property.split(":", 2);
            if (parts[0].equals("null"))
                throw new IllegalArgumentException("Invalid hostname \"null\"");
            if (parts.length == 1)
                throw new IllegalArgumentException("Alias " + description + " as " + property + " malformed, expected hostname:port");
            try {
                int port = Integer.parseInt(parts[1]);
                address = addInetSocketAddress(description, parts[0], port);
                return address;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Alias " + description + " as " + property + " malformed, expected hostname:port with port as a number");
            }
        }

        @NotNull String[] parts = description.split(":", 2);
        if (parts.length == 1)
            throw new IllegalArgumentException("Description " + description + " malformed, expected hostname:port");
        try {
            int port = Integer.parseInt(parts[1]);
            address = addInetSocketAddress(description, parts[0], port);
            return address;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Description " + description + " malformed, expected hostname:port with port as a number");
        }
    }

    @NotNull
    private static InetSocketAddress addInetSocketAddress(String description, @NotNull String hostname, int port) {
        if (port <= 0 || port >= 65536)
            throw new IllegalArgumentException("Invalid port " + port);

        ADDRESS_CACHE.add(description, hostname, port);
        return ADDRESS_CACHE.lookup(description);
    }

    @NotNull
    public static InetSocketAddress createInetSocketAddress(@NotNull String hostname, int port) {
        return hostname.isEmpty() || hostname.equals("*") ? new InetSocketAddress(port) : new InetSocketAddress(hostname, port);
    }

    public static ChronicleSocketChannel createSocketChannel(@NotNull String description) throws IOException {
        return createSocketChannel(false, description);
    }

    public static ChronicleSocketChannel createSocketChannel(boolean isNative, @NotNull String description) throws IOException {
        return wrap(isNative, lookup(description));
    }

    public static void dumpAllSocketChannels() {
        lookupTable().forEach((s, inetSocketAddress) -> Jvm.startup().on(TCPRegistry.class, s + ": " + inetSocketAddress.toString()));
    }

    public static void assertAllServersStopped(final long timeout, final TimeUnit unit) {
        long endtime = System.currentTimeMillis() + unit.toMillis(timeout);
        @NotNull List<ChronicleServerSocketChannel> closed = new ArrayList<>();
        for (@NotNull Map.Entry<String, ChronicleServerSocketChannel> entry : DESC_TO_SERVER_SOCKET_CHANNEL_MAP.entrySet()) {
            if (entry.getValue().isOpen())
                closed.add(entry.getValue());

        }
        lookupTable().clear();
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.clear();
        if (closed.isEmpty())
            return;

        do {
            Jvm.pause(1);

            Iterator<ChronicleServerSocketChannel> iterator = closed.iterator();
            while (iterator.hasNext()) {
                ChronicleServerSocketChannel next = iterator.next();
                if (!next.isOpen()) {

                    iterator.remove();
                }

                if (closed.isEmpty())
                    return;
            }
        }
        while (System.currentTimeMillis() < endtime);

        Closeable.closeQuietly(closed);
        Jvm.pause(500);

        StringBuilder e = new StringBuilder();
        String sep = "";
        for (final ChronicleServerSocketChannel serverSocketChannel : closed) {
            if (!serverSocketChannel.isOpen())
                continue;
            try {
                SocketAddress address = serverSocketChannel.getLocalAddress();
                e.append(sep).append(address);
                sep = ",";
            } catch (ClosedChannelException cce) {
                // ignored
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        if (e.length() > 0)
            throw new AssertionError("Had to stop " + e);
    }

    private static synchronized HostnamePortLookupTable lookupTable() {
        if (lookupTable == null) {
            createNewLookupTable();
        }
        return lookupTable;
    }

    private static void createNewLookupTable() {
        try {
            final Class<?> lookupTableClass = Class.forName(Jvm.getProperty(TCP_REGISTRY_LOOKUP_TABLE_IMPLEMENTATION_PROPERTY,
                    ProcessLocalHostnamePortLookupTable.class.getName()));
            lookupTable = (HostnamePortLookupTable) lookupTableClass.getConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException("Error creating lookup table", ex);
        }
    }
}
