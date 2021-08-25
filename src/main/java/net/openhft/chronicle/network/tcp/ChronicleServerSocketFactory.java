package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.util.ObjectUtils;

public class ChronicleServerSocketFactory {

    public static ChronicleServerSocketChannel open(String hostPort) {
        return new VanillaChronicleServerSocketChannel(hostPort);
    }

    public static ChronicleServerSocketChannel openNative() {
        String className = "software.chronicle.network.impl.NativeChronicleServerSocketChannel";
        return ObjectUtils.newInstance(className);
    }
}
