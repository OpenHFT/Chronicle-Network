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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class VanillaChronicleServerSocketChannel implements ChronicleServerSocketChannel {

    final ServerSocketChannel ssc;
    private String hostPort;

    public VanillaChronicleServerSocketChannel() {
        try {
            ssc = ServerSocketChannel.open();
            hostPort = ssc.getLocalAddress().toString();
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    public VanillaChronicleServerSocketChannel(String hostPort) {
        try {
            ssc = ServerSocketChannel.open();
            this.hostPort = hostPort;
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public ChronicleSocketChannel accept() throws IOException {
        ssc.configureBlocking(true);
        SocketChannel accept = ssc.accept();
        return ChronicleSocketChannelFactory.wrap(accept);
    }

    @Override
    public boolean isOpen() {
        return ssc.isOpen();
    }

    @Override
    public ChronicleServerSocket socket() {

        return new ChronicleServerSocket() {

            @Override
            public int getLocalPort() {
                return ssc.socket().getLocalPort();
            }

            @Override
            public void close() {
                Closeable.closeQuietly(ssc.socket());
            }

            @Override
            public void setReuseAddress(final boolean b) throws SocketException {
                ssc.socket().setReuseAddress(b);
            }

            @Override
            public SocketAddress getLocalSocketAddress() {
                return ssc.socket().getLocalSocketAddress();
            }
        };
    }

    @Override
    public void close() {
        Closeable.closeQuietly(ssc);
    }

    @Override
    public void bind(final InetSocketAddress address) throws IOException {
        ssc.bind(address);
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return ssc.getLocalAddress();
    }

    @Override
    public void setOption(final SocketOption<Boolean> soReuseaddr, final boolean b) throws IOException {
        ssc.setOption(soReuseaddr, b);
    }

    @Override
    public void configureBlocking(final boolean configureBlocking) throws IOException {
        ssc.configureBlocking(configureBlocking);
    }

    @Override
    public String hostPort() {
        return hostPort;
    }

    public VanillaChronicleServerSocketChannel hostPort(String hostPort) {
        this.hostPort = hostPort;
        return this;
    }

    @Override
    public String toString() {
        return "VanillaChronicleServerSocketChannel{" +
                "ssc=" + ssc + ", " +
                "hostPort=" + hostPort +
                '}';
    }
}
