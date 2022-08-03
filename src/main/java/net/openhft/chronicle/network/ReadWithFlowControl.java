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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static net.openhft.chronicle.wire.Wires.lengthOf;

/**
 * provides flow control and back pressure to the socket writer, so that only just one message is read at a time from the TCP/IP socket This class
 * is only used in data grid
 */
public final class ReadWithFlowControl implements TcpEventHandler.SocketReader {

    private int len = 0;
    private boolean hasReadLen = false;
    private int position = 0;
    private int limit = 4;
    private int rawLen;

    /**
     * reads just a single message from the socket
     */
    public int read(@NotNull final ChronicleSocketChannel socketChannel, @NotNull final Bytes<ByteBuffer> bytes) throws IOException {
        ByteBuffer bb = requireNonNull(bytes.underlyingObject());
        bb.limit(limit);
        bb.position(position);
        if (hasReadLen) {
            // write the len
            bytes.writeInt(0, rawLen);
        } else {
            // read the len
            socketChannel.read(bb);

            if (bb.position() < bb.limit())
                return 0;
            rawLen = bytes.readInt(this.len);
            len = lengthOf(rawLen);
            bytes.ensureCapacity(this.len + 4L);
            bb = requireNonNull(bytes.underlyingObject());
            limit(this.len + 8);
            position(4);
            limit(len + 4);
            bb.limit(limit);
            hasReadLen = true;
        }

        socketChannel.read(bb);

        // we can read the message, now read the len of the next message
        if (bb.position() == len + 4) {
            position(0);
            limit(4);
            final int result = len;
            bytes.readPositionRemaining(0, len);
            hasReadLen = false;
            len = 0;
            // read all the data from the buffer
            return result;
        }

        // we can read the message and the len
        if (bb.position() == len + 8) {
            position(4);
            final int result = len;
            rawLen = bytes.readInt(this.len);
            this.len = lengthOf(rawLen);
            limit(this.len + 4);
            bytes.ensureCapacity(this.len + 4L);
            bb = requireNonNull(bytes.underlyingObject());
            bb.position(len + 4);
            bytes.readPositionRemaining(0, result);
            hasReadLen = true;

            return result;
        }

        if (bb.position() > len + 4)
            throw new IllegalStateException("an error has occurred, position=" + bb.position());

        return 0;
    }

    private void limit(final int limit) {
        this.limit = limit;
    }

    private void position(final int position) {
        this.position = position;
    }
}