/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class VanillaSocketChannel extends AbstractCloseable implements ChronicleSocketChannel {
    protected final SocketChannel socketChannel;

    public VanillaSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        singleThreadedCheckDisabled(true);
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.read(byteBuffer);
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.write(byteBuffer);
    }

    @Override
    public long write(ByteBuffer[] byteBuffer) throws IOException {
        return socketChannel.write(byteBuffer);
    }

    @Override
    public ChronicleSocket socket() {
        return ChronicleSocketFactory.toChronicleSocket(socketChannel.socket());
    }

    @Override
    public void configureBlocking(boolean blocking) throws IOException {
        socketChannel.configureBlocking(blocking);
    }

    @Override
    public void connect(final InetSocketAddress socketAddress) throws IOException {
        socketChannel.connect(socketAddress);
    }

    @Override
    public void register(final Selector selector, final int opConnect) throws ClosedChannelException {
        socketChannel.register(selector, opConnect);
    }

    @Override
    public boolean finishConnect() throws IOException {
        return socketChannel.finishConnect();
    }

    @Override
    public void setOption(final SocketOption<Boolean> soReuseaddr, final boolean b) throws IOException {
        socketChannel.setOption(soReuseaddr, b);
    }

    @Override
    public InetSocketAddress getRemoteAddress() throws IORuntimeException {
        try {
            return (InetSocketAddress) socketChannel.getRemoteAddress();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() throws IORuntimeException {
        try {
            return (InetSocketAddress) socketChannel.getLocalAddress();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isBlocking() {
        return socketChannel.isBlocking();
    }

    @Override
    protected boolean shouldPerformCloseInBackground() {
        return true;
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(socketChannel);
    }

    @Override
    public String toString() {
        return "VanillaSocketChannel{" +
                "socketChannel=" + socketChannel +
                '}';
    }
}
