/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public interface NetworkStatsListener<N extends NetworkContext> extends Closeable {

    /**
     * notifies the NetworkStatsListener of the host and port based on the SocketChannel
     *
     * @param sc SocketChannel
     * @param nl NetworkStatsListener
     */
    static void notifyHostPort(@Nullable final SocketChannel sc, @NotNull final NetworkStatsListener nl) {
        if (sc != null && sc.socket() != null
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

    default void procPrefix(String procPrefix) { }

}
