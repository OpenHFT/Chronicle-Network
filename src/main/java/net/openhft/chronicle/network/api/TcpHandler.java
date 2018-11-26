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

package net.openhft.chronicle.network.api;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.ClientClosedProvider;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/*
 * Created by peter.lawrey on 22/01/15.
 */
@FunctionalInterface
public interface TcpHandler<N extends NetworkContext> extends ClientClosedProvider, Closeable {

    /**
     * The server reads the bytes {@code in} from the client and sends a response {@code out} back
     * to the client.
     *
     * @param in  the bytes send from the client
     * @param out the response send back to the client
     */
    void process(@NotNull Bytes in, @NotNull Bytes out, N nc);

    default void sendHeartBeat(Bytes out, SessionDetailsProvider sessionDetails) {
    }

    default void onEndOfConnection(boolean heartbeatTimeOut) {
    }

    @Override
    default void close() {
    }

    default void onReadTime(long readTimeNS, final ByteBuffer inBB, final int position, final int limit) {
    }

    default void onWriteTime(long writeTimeNS,
                             final ByteBuffer byteBuffer,
                             final int start,
                             final int position) {
    }

    default void onReadComplete() {
    }
}
