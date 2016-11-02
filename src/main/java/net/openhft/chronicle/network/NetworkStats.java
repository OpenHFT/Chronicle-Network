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

import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.util.UUID;

/**
 * used to collected stats about the network activity
 */
public interface NetworkStats<T extends NetworkStats> extends ReadMarshallable, WriteMarshallable {

    T userId(String userId);

    /**
     * @return bytes per second
     */
    long writeBps();

    T writeBps(long writeBps);

    /**
     * @return bytes per second
     */
    long readBps();

    T readBps(long readBps);

    /**
     * @return how many times was a socket read attempted within a second, a low number here is an
     * indication that your system may be struggling to keep up
     */
    long socketPollCountPerSecond();

    T socketPollCountPerSecond(long socketPollCountPerSecond);

    long timestamp();

    T timestamp(long timestamp);

    void remotePort(int port);

    T remoteHostName(String hostName);

    String userId();

    /**
     * @return the identifier of this instance of engine
     */
    long localIdentifier();

    T localIdentifier(long localIdentifier);


    /**
     * the identifier of the remote instance of engine
     */
    long remoteIdentifier();

    T remoteIdentifier(long remoteIdentifier);

    void clientId(UUID clientId);

    UUID clientId();

    String remoteHostName();

    int remotePort();

    boolean isConnected();

    void isConnected(boolean isConnected);
}