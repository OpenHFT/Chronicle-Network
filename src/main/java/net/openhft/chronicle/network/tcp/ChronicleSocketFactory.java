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

import net.openhft.chronicle.network.connection.TcpChannelHub;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public enum ChronicleSocketFactory {
    ;

    public static ChronicleSocket toChronicleSocket(Socket socket) {
        return new ChronicleSocket() {

            @Override
            public void setTcpNoDelay(final boolean tcpNoDelay) throws SocketException {
                TcpChannelHub.setTcpNoDelay(socket, tcpNoDelay);
            }

            @Override
            public int getReceiveBufferSize() throws SocketException {
                return socket.getReceiveBufferSize();
            }

            @Override
            public void setReceiveBufferSize(final int tcpBuffer) throws SocketException {
                socket.setReceiveBufferSize(tcpBuffer);
            }

            @Override
            public int getSendBufferSize() throws SocketException {
                return socket.getSendBufferSize();
            }

            @Override
            public void setSendBufferSize(final int tcpBuffer) throws SocketException {
                socket.setSendBufferSize(tcpBuffer);
            }

            @Override
            public void setSoTimeout(final int i) throws SocketException {
                socket.setSoTimeout(i);
            }

            @Override
            public void setSoLinger(final boolean b, final int i) throws SocketException {
                socket.setSoLinger(b, i);
            }

            @Override
            public void shutdownInput() throws IOException {
                socket.shutdownInput();
            }

            @Override
            public void shutdownOutput() throws IOException {
                socket.shutdownOutput();
            }

            @Override
            public Object getRemoteSocketAddress() {
                return socket.getRemoteSocketAddress();
            }

            @Override
            public Object getLocalSocketAddress() {
                return socket.getLocalSocketAddress();
            }

            @Override
            public int getLocalPort() {
                return socket.getLocalPort();
            }
        };
    }
}
