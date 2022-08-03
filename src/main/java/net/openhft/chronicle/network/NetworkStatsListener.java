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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

public interface NetworkStatsListener<N extends NetworkContext<N>> extends Closeable {

    /**
     * notifies the NetworkStatsListener of the host and port based on the SocketChannel
     *
     * @param sc SocketChannel
     * @param nl NetworkStatsListener
     */
    static <N extends NetworkContext<N>> void notifyHostPort(@Nullable final ChronicleSocketChannel sc, @Nullable final NetworkStatsListener<N> nl) {
        if (nl != null && sc != null && sc.socket() != null
                && sc.socket().getRemoteSocketAddress() instanceof InetSocketAddress) {
            @NotNull final InetSocketAddress remoteSocketAddress = (InetSocketAddress) sc.socket()
                    .getRemoteSocketAddress();
            nl.onHostPort(remoteSocketAddress.getHostName(), remoteSocketAddress.getPort());
        }
    }

    void networkContext(N networkContext);

    void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond);

    void onHostPort(String hostName, int port);

    void onRoundTripLatency(long nanosecondLatency);

    default void procPrefix(String procPrefix) {
    }
}
