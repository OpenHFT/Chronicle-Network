/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by peter.lawrey on 15/07/2015.
 */
class NetworkLog {
    private static final Logger LOG =
            LoggerFactory.getLogger(NetworkLog.class.getName());
    @NotNull
    private final String desc;
    private long lastOut = System.currentTimeMillis();

    public NetworkLog(@NotNull SocketChannel channel, String op) {
        try {
            this.desc = op
                    + " " + ((InetSocketAddress) channel.getLocalAddress()).getPort()
                    + " " + ((InetSocketAddress) channel.getRemoteAddress()).getPort();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public void idle() {
        if (!Jvm.isDebug() || !LOG.isDebugEnabled()) return;
        long now = System.currentTimeMillis();
        if (now - lastOut > 2000) {
            lastOut = now;
            Jvm.debug().on(getClass(), desc + " idle");
        }
    }

    public void log(@NotNull ByteBuffer bytes, int start, int end) {
        if (!Jvm.isDebug() || !LOG.isDebugEnabled()) return;

        // avoid inlining this.
        log0(bytes, start, end);
    }

    private void log0(@NotNull ByteBuffer bytes, int start, int end) {
        @NotNull final StringBuilder sb = new StringBuilder(desc);
        sb.append(" len: ").append(end - start).append(" - ");
        if (end - start > 128) {
            for (int i = start; i < start + 64; i++)
                appendByte(bytes, sb, i);
            sb.append(" ... ");
            for (int i = end - 64; i < end; i++)
                appendByte(bytes, sb, i);
        } else {
            for (int i = start; i < end; i++)
                appendByte(bytes, sb, i);
        }

        Jvm.debug().on(getClass(), sb.toString());
    }

    private void appendByte(@NotNull ByteBuffer bytes, @NotNull StringBuilder sb, int i) {
        sb.append(RandomDataInput.charToString[bytes.get(i) & 0xFF]);
    }
}
