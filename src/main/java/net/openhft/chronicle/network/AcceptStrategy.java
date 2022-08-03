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

import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.wire.Marshallable;

import java.io.IOException;

/**
 * Can be used to reject incoming connections e.g. you could implement an AcceptStrategy that checks remote IPs and rejects if not in whitelist
 */
@FunctionalInterface
public interface AcceptStrategy extends Marshallable {

AcceptStrategy ACCEPT_ALL = AcceptStrategies.ACCEPT_ALL;

    /**
     * Determine whether to accept the incoming connection
     *
     * @param ssc
     * @return null to reject the connection, otherwise return the accepted SocketChannel
     * @throws IOException
     */
    ChronicleSocketChannel accept(ChronicleServerSocketChannel ssc) throws IOException;
}
