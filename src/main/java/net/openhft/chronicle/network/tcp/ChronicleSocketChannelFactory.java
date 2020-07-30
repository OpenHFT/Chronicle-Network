package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public enum ChronicleSocketChannelFactory {
    ;
    public static boolean FAST_JAVA8_IO = isFastJava8IO();

    private static boolean isFastJava8IO() {
        boolean fastJava8IO = Jvm.getBoolean("fastJava8IO") && !Jvm.isJava9Plus() && OS.isLinux();
        if (fastJava8IO) System.out.println("FastJava8IO: enabled");
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
        try {
            return (ChronicleSocketChannel) Class.forName("software.chronicle.network.impl.NativeChronicleSocketChannel").newInstance();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }

    }

    @NotNull
    private static ChronicleSocketChannel newFastUnsafeInstance(@NotNull final SocketChannel sc) {
        return new UnsafeFastJ8SocketChannel(sc);
    }

}
