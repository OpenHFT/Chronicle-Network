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
        return wrap(SocketChannel.open());
    }

    public static ChronicleSocketChannel wrap(InetSocketAddress socketAddress) throws IOException {
        return wrap(SocketChannel.open(socketAddress));
    }

    public static ChronicleSocketChannel wrap(@NotNull final SocketChannel sc) {
        return FAST_JAVA8_IO ? newFastInstance(sc) : newInstance(sc);
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

    @NotNull
    private static ChronicleSocketChannel newFastUnsafeInstance(@NotNull final SocketChannel sc) {
        return new UnsafeFastJ8SocketChannel(sc);
    }

}
