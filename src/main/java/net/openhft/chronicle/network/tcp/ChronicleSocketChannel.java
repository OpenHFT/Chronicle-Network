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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.ChronicleSocketChannelBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface ChronicleSocketChannel extends Closeable {

    static ChronicleSocketChannelBuilder builder(InetSocketAddress socketAddress) {
        return new ChronicleSocketChannelBuilder(socketAddress);
    }

    int read(ByteBuffer byteBuffer) throws IOException;

    int write(ByteBuffer byteBuffer) throws IOException;

    long write(ByteBuffer[] byteBuffers) throws IOException;

    void configureBlocking(boolean blocking) throws IOException;

    InetSocketAddress getLocalAddress() throws IOException;

    void bind(InetSocketAddress localAddress) throws IOException;

    InetSocketAddress getRemoteAddress() throws IOException;

    boolean isOpen();

    boolean isBlocking();

    ChronicleSocket socket();

    void connect(InetSocketAddress socketAddress) throws IOException;

    void register(Selector selector, int opConnect) throws ClosedChannelException;

    boolean finishConnect() throws IOException;

    void setOption(SocketOption<Boolean> soReuseaddr, boolean b) throws IOException;

    SocketChannel socketChannel();
}
