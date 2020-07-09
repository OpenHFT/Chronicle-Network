package net.openhft.chronicle.network.tcp;

public class ChronicleServerSocketFactory {

    public static ChronicleServerSocketChannel open() {
        return new VanillaChronicleServerSocketChannel();
    }
}
