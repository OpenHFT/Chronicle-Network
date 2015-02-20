package net.openhft.chronicle.network2;

import net.openhft.chronicle.network2.event.EventHandler;
import net.openhft.chronicle.network2.event.EventLoop;
import net.openhft.chronicle.network2.event.HandlerPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

/**
 * Created by peter on 22/01/15.
 */
public class AcceptorEventHandler implements EventHandler {
    private final Supplier<TcpHandler> tcpHandlerSupplier;
    private EventLoop eventLoop;
    private final ServerSocketChannel ssc;

    private static final Logger LOG = LoggerFactory.getLogger(AcceptorEventHandler.class);

    public AcceptorEventHandler(int port, Supplier<TcpHandler> tcpHandlerSupplier) throws IOException {
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        ssc = ServerSocketChannel.open();
        ssc.socket().setReuseAddress(true);
        ssc.bind(new InetSocketAddress(port));
    }

    public int getLocalPort() throws IOException {
        return ssc.socket().getLocalPort();
    }

    @Override
    public void eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean runOnce()  {
        try {
            SocketChannel sc = ssc.accept();

            if (sc != null)
                eventLoop.addHandler(new TcpEventHandler(sc, tcpHandlerSupplier.get()));
        } catch (Exception e) {
            LOG.error("", e);
            try {
                ssc.close();
            } catch (IOException ignored) {

            }
        }
        return false;
    }

    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    public boolean isDead() {
        return !ssc.isOpen();
    }
}
