package net.openhft.chronicle.network.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import net.openhft.chronicle.threads.Threads;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.openhft.chronicle.network.internal.SocketExceptionUtil.isAConnectionResetException;
import static org.junit.Assert.*;

public class SocketExceptionUtilTest {

    /**
     * Original means of detection
     */
    @Test
    public void isAConnectionResetExceptionReturnsTrueWhenMessageMatches() {
        assertTrue(isAConnectionResetException(new IOException("Connection reset by peer")));
    }

    /**
     * Added by this change https://github.com/openjdk/jdk/commit/3a4d5db248d74020b7448b64c9f0fc072fc80470
     * <p>
     * Thrown in JDK 13 and above
     */
    @Test
    public void isAConnectionResetExceptionReturnsTrueForSocketExceptionWithShorterMessage() {
        assertTrue(isAConnectionResetException(new SocketException("Connection reset")));
    }

    @Test
    public void isAConnectionResetExceptionReturnsFalseForOtherExceptions() {
        assertFalse(isAConnectionResetException(new SocketException("Something else happened")));
        assertFalse(isAConnectionResetException(new IOException("Something else happened")));
    }

    @Test
    public void isAConnectionResetIsRobustAgainstNullMessages() {
        assertFalse(isAConnectionResetException(new IOException()));
    }

    @Test
    public void testConnectionResetDetectionForLocales() throws IOException {
        final Locale originalDefault = Locale.getDefault();
        try {
            Locale.setDefault(Locale.KOREA);
            testConnectionResetDetection();
            Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
            testConnectionResetDetection();
            Locale.setDefault(originalDefault);
            testConnectionResetDetection();
        } finally {
            Locale.setDefault(originalDefault);
        }
    }

    private void testConnectionResetDetection() throws IOException {
        final ChronicleServerSocketChannel serverSocketChannel = TCPRegistry.createServerSocketChannelFor("server-address");
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            // Server logic
            try {
                final ChronicleSocketChannel csc = serverSocketChannel.accept();
                Jvm.pause(100); // make sure the client read has started
                final Socket socket = csc.socketChannel().socket();
                socket.setSoLinger(true, 0);
                socket.close();
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        final ChronicleSocketChannel clientSocketChannel = ChronicleSocketChannelFactory.wrap(false, TCPRegistry.lookup("server-address"));
        try {
            clientSocketChannel.read(ByteBuffer.allocate(1000));
        } catch (IOException e) {
            assertTrue(isAConnectionResetException(e));
        }
        Threads.shutdown(executorService);
    }
}