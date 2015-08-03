/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Created by peter on 03/07/15.
 */
public enum TCPRegistry {
    ;
    static final Map<String, InetSocketAddress> HOSTNAME_PORT_ALIAS = new ConcurrentSkipListMap<>();
    static final Map<String, ServerSocketChannel> DESC_TO_SERVER_SOCKET_CHANNEL_MAP = new ConcurrentSkipListMap<>();

    public static void reset() {
        for (ServerSocketChannel ssc : DESC_TO_SERVER_SOCKET_CHANNEL_MAP.values()) {
            closeQuietly(ssc);
        }
        HOSTNAME_PORT_ALIAS.clear();
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.clear();
    }

    public static void assertAllServersStopped() {
        List<String> closed = new ArrayList<>();
        for (Map.Entry<String, ServerSocketChannel> entry : DESC_TO_SERVER_SOCKET_CHANNEL_MAP.entrySet()) {
            if (entry.getValue().isOpen())
                closed.add(entry.toString());
            closeQuietly(entry.getValue());
        }
        HOSTNAME_PORT_ALIAS.clear();
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.clear();
        if (!closed.isEmpty())
            throw new AssertionError("Had to stop " + closed);
    }

    public static void setAlias(String name, @NotNull String hostname, int port) {
        HOSTNAME_PORT_ALIAS.put(name, new InetSocketAddress(hostname, port));
    }

    public static void createServerSocketChannelFor(@NotNull String... descriptions) throws IOException {
        for (String description : descriptions) {
            InetSocketAddress address;
            if (description.contains(":")) {
                String[] split = description.trim().split(":");
                String host = split[0];
                int port = Integer.parseInt(split[1]);
                address = createInetSocketAddress(host, port);
            } else {
                address = new InetSocketAddress(0);
            }
            createSSC(description, address);
        }
    }

    public static void createSSC(String description, InetSocketAddress address) throws IOException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.socket().setReuseAddress(true);
        ssc.bind(address);
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.put(description, ssc);
        HOSTNAME_PORT_ALIAS.put(description, (InetSocketAddress) ssc.socket().getLocalSocketAddress());
    }

    public static ServerSocketChannel acquireServerSocketChannel(@NotNull String description) throws IOException {
        ServerSocketChannel ssc = DESC_TO_SERVER_SOCKET_CHANNEL_MAP.get(description);
        if (ssc != null && ssc.isOpen())
            return ssc;
        InetSocketAddress address = lookup(description);
        ssc = ServerSocketChannel.open();
        ssc.socket().setReuseAddress(true);
        ssc.bind(address);
        DESC_TO_SERVER_SOCKET_CHANNEL_MAP.put(description, ssc);
        return ssc;
    }

    public static InetSocketAddress lookup(@NotNull String description) {
        InetSocketAddress address = HOSTNAME_PORT_ALIAS.get(description);
        if (address != null)
            return address;
        String property = System.getProperty(description);
        if (property != null) {
            String[] parts = property.split(":", 2);
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

        String[] parts = description.split(":", 2);
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

        InetSocketAddress address = createInetSocketAddress(hostname, port);
        HOSTNAME_PORT_ALIAS.put(description, address);
        return address;
    }

    @NotNull
    private static InetSocketAddress createInetSocketAddress(@NotNull String hostname, int port) {
        return hostname.isEmpty() || hostname.equals("*") ? new InetSocketAddress(port) : new InetSocketAddress(hostname, port);
    }

    public static SocketChannel createSocketChannel(@NotNull String description) throws IOException {
        return SocketChannel.open(lookup(description));
    }
}
