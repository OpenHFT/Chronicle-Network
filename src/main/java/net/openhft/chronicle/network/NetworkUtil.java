package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import static net.openhft.chronicle.core.Jvm.getInteger;

public enum NetworkUtil {
    ;
    public static final int TCP_BUFFER_SIZE = getTcpBufferSize();
    public static final int TCP_SAFE_SIZE = getInteger("tcp.safe.size", 128 << 10);
    public static final boolean TCP_USE_PADDING = Jvm.getBoolean("tcp.use.padding", false);

    private static int getTcpBufferSize() {
        final String sizeStr = Jvm.getProperty("TcpEventHandler.tcpBufferSize");
        if (sizeStr != null && !sizeStr.isEmpty())
            try {
                final int size = Integer.parseInt(sizeStr);
                if (size >= 64 << 10)
                    return size;
            } catch (Exception e) {
                Jvm.warn().on(NetworkUtil.class, "Unable to parse tcpBufferSize=" + sizeStr, e);
            }
        try {
            try (ServerSocket ss = new ServerSocket(0)) {
                try (Socket s = new Socket("localhost", ss.getLocalPort())) {
                    s.setReceiveBufferSize(4 << 20);
                    s.setSendBufferSize(4 << 20);
                    final int size = Math.min(s.getReceiveBufferSize(), s.getSendBufferSize());
                    (size >= 128 << 10 ? Jvm.debug() : Jvm.warn())
                            .on(NetworkUtil.class, "tcpBufferSize = " + size / 1024.0 + " KiB");
                    return size;
                }
            }
        } catch (Exception e) {
            throw new IORuntimeException(e); // problem with networking subsystem.
        }
    }

    public static void setTcpNoDelay(Socket socket, boolean tcpNoDelay) throws SocketException {
        for (int i = 10; i >= 0; i--) {
            try {

                socket.setTcpNoDelay(tcpNoDelay);
            } catch (SocketException se) {
                if (i == 0)
                    throw se;
                Jvm.pause(1);
            }
        }
    }
}
