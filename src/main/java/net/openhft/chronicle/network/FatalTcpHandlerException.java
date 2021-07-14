package net.openhft.chronicle.network;

/**
 * Thrown when a TCP handler encounters an unrecoverable error
 */
public class FatalTcpHandlerException extends RuntimeException {

    public FatalTcpHandlerException(String s) {
        super(s);
    }
}
