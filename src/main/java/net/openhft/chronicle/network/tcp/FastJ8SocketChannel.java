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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class FastJ8SocketChannel extends VanillaSocketChannel {
    final FileDescriptor fd;
    private final Object readLock;
    volatile boolean open;
    private volatile boolean blocking;

    public FastJ8SocketChannel(SocketChannel socketChannel) {
        super(socketChannel);
        fd = Jvm.getValue(socketChannel, "fd");
        open = socketChannel.isOpen();
        blocking = socketChannel.isBlocking();
        readLock = Jvm.getValue(socketChannel, "readLock");
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        throwExceptionIfClosed();

        if (buf == null)
            throw new NullPointerException();

        if (isBlocking() || isClosed() || !IOTools.isDirectBuffer(buf) || !super.isOpen())
            return super.read(buf);
        return read0(buf);
    }

    int read0(ByteBuffer buf) throws IOException {
        synchronized (readLock) {
            if (Thread.interrupted())
                throw new IOException(new InterruptedException());

            return readInternal(buf);
        }
    }

    @Override
    public void configureBlocking(boolean blocking) throws IOException {
        super.configureBlocking(blocking);
        this.blocking = super.isBlocking();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isBlocking() {
        return blocking;
    }

    @Override
    protected void performClose() {
        super.performClose();
        open = false;
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        throwExceptionIfClosed();

        try {
            int write = super.write(byteBuffer);
            open &= write >= 0;
            return write;

        } catch (Exception e) {
            open = super.isOpen();
            throw e;
        }
    }

    int readInternal(ByteBuffer buf) throws IOException {
        int n = OS.read0(fd, IOTools.addressFor(buf) + buf.position(), buf.remaining());
        if ((n == IOTools.IOSTATUS_INTERRUPTED) && socketChannel.isOpen()) {
            // The system call was interrupted but the channel
            // is still open, so retry
            return 0;
        }
        int ret = IOTools.normaliseIOStatus(n);
        if (ret > 0)
            buf.position(buf.position() + ret);
        else if (ret < 0)
            open = false;
        return ret;
    }
}
