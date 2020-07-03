/*
 * Copyright 2016-2020 Chronicle Software
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
import net.openhft.chronicle.core.io.Closeable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public interface ISocketChannel extends Closeable {

    boolean FAST_JAVA8_IO = isFastJava8IO();

    static boolean isFastJava8IO() {
        boolean fastJava8IO = Jvm.getBoolean("fastJava8IO") && !Jvm.isJava9Plus() && OS.isLinux();
        if (fastJava8IO) System.out.println("FastJava8IO: " + fastJava8IO);
        return fastJava8IO;
    }

    @NotNull
    static ISocketChannel wrap(ChronicleSocketChannel sc) {
        assert sc != null;
        return FAST_JAVA8_IO ? new FastJ8SocketChannel(sc) : new VanillaSocketChannel(sc);
    }

    @NotNull
    static ISocketChannel wrapUnsafe(ChronicleSocketChannel sc) {
        assert sc != null;
        return FAST_JAVA8_IO ? new UnsafeFastJ8SocketChannel(sc) : new VanillaSocketChannel(sc);
    }

    @NotNull
    ChronicleSocketChannel socketChannel();

    int read(ByteBuffer byteBuffer) throws IOException;

    int write(ByteBuffer byteBuffer) throws IOException;

    long write(ByteBuffer[] byteBuffer) throws IOException;

    @NotNull
    ChronicleSocket socket();

    void configureBlocking(boolean blocking) throws IOException;

    @NotNull
    InetSocketAddress getRemoteAddress() throws IOException;

    @NotNull
    InetSocketAddress getLocalAddress() throws IOException;

    /**
     * As this socket can be closed in a background thread it can be both isClosed() and isOpen() if close() has been called but it hasn't actually
     * been closed
     *
     * @return is the underlying socket open
     * @see net.openhft.chronicle.core.io.QueryCloseable
     */
    boolean isOpen();

    /**
     * As this socket can be closed in a background thread it can be both isClosed() and isOpen() if close() has been called but it hasn't actually
     * been closed
     *
     * @return has close been called
     */
    // This breaks on Java 11
    // @Override
    // boolean isClosed();

    boolean isBlocking();
}
