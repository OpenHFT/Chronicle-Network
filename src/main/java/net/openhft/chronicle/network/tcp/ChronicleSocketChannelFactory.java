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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.ObjectUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public enum ChronicleSocketChannelFactory {
    ;
    public static final boolean FAST_JAVA8_IO = isFastJava8IO();

    private static boolean isFastJava8IO() {
        boolean fastJava8IO = Jvm.getBoolean("fastJava8IO") && !Jvm.isJava9Plus() && OS.isLinux();
        if (fastJava8IO) Jvm.startup().on(ChronicleSocketChannelFactory.class, "FastJava8IO: enabled");
        return fastJava8IO;
    }

    public static ChronicleSocketChannel wrap() throws IOException {
        return wrap(false, SocketChannel.open());
    }

    public static ChronicleSocketChannel wrap(InetSocketAddress socketAddress) throws IOException {
        return wrap(false, SocketChannel.open(socketAddress));
    }

    public static ChronicleSocketChannel wrap(boolean isNative, InetSocketAddress socketAddress) throws IOException {
        SocketChannel sc = SocketChannel.open(socketAddress);
        return wrap(isNative, sc);
    }

    public static ChronicleSocketChannel wrap(@NotNull final SocketChannel sc) {
        return wrap(false, sc);
    }

    public static ChronicleSocketChannel wrap(boolean isNative, @NotNull final SocketChannel sc) {
        return isNative ? newNativeInstance(sc) : FAST_JAVA8_IO ? newFastInstance(sc) : newInstance(sc);
    }

    public static ChronicleSocketChannel wrapUnsafe(@NotNull final SocketChannel sc) {
        return FAST_JAVA8_IO ? newFastUnsafeInstance(sc) : newInstance(sc);
    }

    @NotNull
    private static ChronicleSocketChannel newInstance(@NotNull final SocketChannel sc) {
        return new VanillaSocketChannel(sc);
    }

    @NotNull
    private static ChronicleSocketChannel newFastInstance(@NotNull final SocketChannel sc) {
        return new FastJ8SocketChannel(sc);
    }

    private static ChronicleSocketChannel newNativeInstance(@NotNull final SocketChannel sc) {
        String className = "software.chronicle.network.impl.NativeChronicleSocketChannel";
        return ObjectUtils.newInstance(className);
    }

    @NotNull
    private static ChronicleSocketChannel newFastUnsafeInstance(@NotNull final SocketChannel sc) {
        return new UnsafeFastJ8SocketChannel(sc);
    }
}
