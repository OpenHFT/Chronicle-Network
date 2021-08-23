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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SocketChannel;

/**
 * Doesn't do any locking so can only be used in a single-threaded manner
 */
public class UnsafeFastJ8SocketChannel extends FastJ8SocketChannel {
    public UnsafeFastJ8SocketChannel(SocketChannel socketChannel) {
        super(socketChannel);
        disableThreadSafetyCheck(true);
    }

    @Override
    int read0(ByteBuffer buf) throws IOException {
        return readInternal(buf);
    }

    @Override
    public int write(ByteBuffer buf) throws IOException {
        if (buf == null)
            throw new NullPointerException();

        if (isBlocking() || isClosed() || !IOTools.isDirectBuffer(buf))
            return super.write(buf);

        return writeInternal(buf);
    }

    private int writeInternal(ByteBuffer buf) throws IOException {
        int pos = buf.position();
        int lim = buf.limit();
        int len = lim <= pos ? 0 : lim - pos;
        int res = OS.write0(fd, IOTools.addressFor(buf) + pos, len);
        if (res > 0)
            buf.position(pos + res);
        if ((res == IOTools.IOSTATUS_INTERRUPTED) && socketChannel.isOpen()) {
            // The system call was interrupted but the channel
            // is still open, so retry
            return 0;
        }
        res = IOTools.normaliseIOStatus(res);
        if (res < 0)
            open = false;
        if (res <= 0 && !socketChannel.isOpen())
            throw new AsynchronousCloseException();

        return res;
    }
}
