package mina.examples;

/**
 * @author Rob Austin.
 */

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.prefixedstring.PrefixedStringCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * (<strong>Entry Point</strong>) Starts SumUp client.
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Client {
    private static final String HOSTNAME = "localhost";

    private static final int PORT = 9123;

    private static final long CONNECT_TIMEOUT = 30 * 1000L; // 30 seconds


    public static void main(String[] args) throws Throwable {
        NioSocketConnector connector = new NioSocketConnector();

        connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new
                PrefixedStringCodecFactory(Charset.forName("ISO-8859-1"))));


        connector.setHandler(new IoHandlerAdapter() {

            int bytesReceived = 0;
            long startTime;
            final int bufferSize = 64;
            byte[] payload = new byte[bufferSize];
            final String payloadStr = new String(payload);

            int i;

            @Override
            public void sessionOpened(IoSession session) {
                startTime = System.nanoTime();
                session.write(payloadStr);
            }

            @Override
            public void sessionClosed(IoSession session) {
                System.out.println("closed");
            }

            @Override
            public void messageReceived(IoSession session, Object message) {

                bytesReceived += message.toString().length();

                if (i++ % 10000 == 0)
                    System.out.print(".");

                session.write(payloadStr);

                if (TimeUnit.NANOSECONDS.toSeconds(System
                        .nanoTime() - startTime) >= 10) {
                    long time = System.nanoTime() - startTime;
                    System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                            bytesReceived / time);
                    session.close(true);
                }

            }

            @Override
            public void messageSent(IoSession session, Object message) {

            }

            @Override
            public void exceptionCaught(IoSession session, Throwable cause) {
                cause.printStackTrace();
                session.close(true);
            }
        });
        IoSession session;

        for (; ; ) {
            try {
                ConnectFuture future = connector.connect(new InetSocketAddress(HOSTNAME, PORT));
                future.awaitUninterruptibly();
                session = future.getSession();
                break;
            } catch (RuntimeIoException e) {
                e.printStackTrace();
                Thread.sleep(5000);
            }
        }

        // wait until the summation is done
        session.getCloseFuture().awaitUninterruptibly();
        connector.dispose();
    }
}