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
        return isALinuxJava12OrLessConnectionResetException(e)
                || isAWindowsConnectionResetException(e)
                || isALinuxJava13OrGreaterConnectionResetException(e);
    }

    private static boolean isALinuxJava13OrGreaterConnectionResetException(IOException e) {
        return e.getClass().equals(SocketException.class) && "Connection reset".equals(e.getMessage());
    }

    private static boolean isAWindowsConnectionResetException(IOException e) {
        return e.getClass().equals(IOException.class) && "An existing connection was forcibly closed by the remote host".equals(e.getMessage());
    }

    private static boolean isALinuxJava12OrLessConnectionResetException(IOException e) {
        return e.getClass().equals(IOException.class) && "Connection reset by peer".equals(e.getMessage());
    }
}
