package net.openhft.performance.tests.third.party.frameworks.mina;

/**
 * @author Rob Austin.
 */

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MinaEchoServer {


    static final int PORT = Integer.parseInt(System.getProperty("port", "9120"));

    public static void main(String[] args) throws IOException {

        IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.setHandler(new IoHandlerAdapter() {
            @Override
            public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
                cause.printStackTrace();
            }

            @Override
            public void messageReceived(IoSession session, Object message) throws Exception {
                session.write(message);
            }

            @Override
            public void sessionIdle(IoSession session, IdleStatus status) throws Exception {

            }
        });

        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

        acceptor.bind(new InetSocketAddress(PORT));
    }
}