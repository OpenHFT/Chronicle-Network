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

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class AbstractSocketBufferHandler implements BufferHandler {
    private final ChronicleSocketChannel channel;

    AbstractSocketBufferHandler(final ChronicleSocketChannel socketChannel) {
        this.channel = socketChannel;
    }

    @Override
    public int readData(final ByteBuffer target) throws IOException {
        return channel.read(target);
    }

    @Override
    public int writeData(final ByteBuffer encrypted) throws IOException {
        return channel.write(encrypted);
    }
}
