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

/**
 * Adapter to make it easier to make inline NetworkStatsListeners
 *
 * @param <N> the NetworkContext type
 */
public class NetworkStatsAdapter<N extends NetworkContext<N>> implements NetworkStatsListener<N> {

    @Override
    public void networkContext(N networkContext) {
    }

    @Override
    public void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond) {
    }

    @Override
    public void onHostPort(String hostName, int port) {
    }

    @Override
    public void onRoundTripLatency(long nanosecondLatency) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
