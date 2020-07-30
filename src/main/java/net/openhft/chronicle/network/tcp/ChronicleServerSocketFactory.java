package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.core.Jvm;

public class ChronicleServerSocketFactory {

    public static ChronicleServerSocketChannel open() {
        return new VanillaChronicleServerSocketChannel();
    }

    public static ChronicleServerSocketChannel openNative() {
        try {
            return (ChronicleServerSocketChannel) Class.forName("software.chronicle.network.impl.NativeChronicleServerSocketChannel").newInstance();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }

}
