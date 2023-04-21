package net.openhft.chronicle.network.util;

import java.io.IOException;
import java.net.ServerSocket;

public enum TestUtil {
    ;

    /**
     * Get a port number that's most likely available
     *
     * @return a port number that's available
     */
    public static int getAvailablePortNumber() {
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
