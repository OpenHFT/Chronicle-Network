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

package net.openhft.chronicle.network.tcp;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketOptions;

public interface ChronicleSocket {

    void setTcpNoDelay(final boolean tcpNoDelay) throws SocketException;

    int getReceiveBufferSize() throws SocketException;

    void setReceiveBufferSize(final int tcpBuffer) throws SocketException;

    int getSendBufferSize() throws SocketException;

    void setSendBufferSize(final int tcpBuffer) throws SocketException;

    void setSoTimeout(int i) throws SocketException;

    /**
     * Enable/disable {@link SocketOptions#SO_LINGER SO_LINGER} with the specified linger time in seconds. The maximum timeout value is platform
     * specific.
     *
     * The setting only affects socket close.
     *
     * @param on     whether or not to linger on.
     * @param linger how long to linger for, if on is true.
     * @throws SocketException          if there is an error in the underlying protocol, such as a TCP error.
     * @throws IllegalArgumentException if the linger value is negative.
     * @since JDK1.1
     */
    void setSoLinger(boolean on, int linger) throws SocketException;

    void shutdownInput() throws IOException;

    void shutdownOutput() throws IOException;

    Object getRemoteSocketAddress();

    Object getLocalSocketAddress();

    int getLocalPort();

}
