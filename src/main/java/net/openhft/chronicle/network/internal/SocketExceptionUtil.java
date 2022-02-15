package net.openhft.chronicle.network.internal;

import java.io.IOException;
import java.net.SocketException;

public final class SocketExceptionUtil {

    private SocketExceptionUtil() {
    }

    /**
     * Is the passed exception one that results from reading from or writing to a reset
     * connection?
     * <p>
     * NOTE: This is not reliable and shouldn't be used for anything critical. We use it
     * to make logging less noisy, false negatives are acceptable and expected. It should
     * not produce false positives, but there's no guarantees it doesn't.
     *
     * @param e The exception
     * @return true if the exception is one that signifies the connection was reset
     */
    public static boolean isAConnectionResetException(IOException e) {
        String message = e.getMessage();
        return message != null &&
                (message.equals("Connection reset by peer")
                        || e instanceof SocketException && message.equals("Connection reset"));
    }
}
