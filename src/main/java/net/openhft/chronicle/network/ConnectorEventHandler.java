package net.openhft.chronicle.network;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.wire.WireType.TEXT;

/**
 * Created by daniel on 09/02/2016. This class handles the creation, running and monitoring of
 * client connections
 */
public class ConnectorEventHandler implements EventHandler, Closeable {

    @NotNull
    private final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier;
    @NotNull
    private final Supplier<SessionDetailsProvider> sessionDetailsSupplier;

    private final Map<String, SocketChannel> descriptionToChannel = new ConcurrentHashMap<>();
    private final Pauser pauser = new LongPauser(0, 0, 5, 5, TimeUnit.SECONDS);
    private EventLoop eventLoop;
    private Map<String, ConnectionDetails> nameToConnectionDetails;


    public ConnectorEventHandler(@NotNull Map<String, ConnectionDetails> nameToConnectionDetails,
                                 @NotNull final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier,
                                 @NotNull final Supplier<SessionDetailsProvider>
                                         sessionDetailsSupplier) throws IOException {
        this.nameToConnectionDetails = nameToConnectionDetails;
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        this.sessionDetailsSupplier = sessionDetailsSupplier;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        nameToConnectionDetails.forEach((k, v) -> {
            try {
                SocketChannel socketChannel = descriptionToChannel.get(k);

                if (socketChannel == null) {
                    if (v.isDisable()) {
                        //we shouldn't create anything
                        return;
                    }
                    socketChannel = TCPRegistry.createSocketChannel(v.getHostNameDescription());
                    descriptionToChannel.put(k, socketChannel);
                    v.setConnected(true);
                    final SessionDetailsProvider sessionDetails = sessionDetailsSupplier.get();

                    sessionDetails.clientAddress((InetSocketAddress) socketChannel.getRemoteAddress());
                    NetworkContext nc = new VanillaNetworkContext();
                    nc.wireOutPublisher(new VanillaWireOutPublisher(TEXT));
                    nc.socketChannel(socketChannel);
                    final TcpEventHandler evntHandler = new TcpEventHandler(nc);
                    evntHandler.tcpHandler(tcpHandlerSupplier.apply(v));
                    eventLoop.addHandler(evntHandler);
                } else if (socketChannel.isOpen()) {
                    //the socketChannel is doing fine
                    //check whether it should be disabled
                    if (v.isDisable()) {
                        socketChannel.close();
                        v.setConnected(false);
                        descriptionToChannel.remove(k);
                    }
                } else {
                    //the socketChannel has disconnected
                    v.setConnected(false);
                    descriptionToChannel.remove(k);
                }
            } catch (ConnectException e) {
                //Not a problem try again next time round
                System.out.println(k + e.getMessage());
            } catch (IOException e) {
                System.out.println(k + e.getMessage());
                e.printStackTrace();
            }
        });

        pauser.pause();

        return false;
    }

    public void updateConnectionDetails(ConnectionDetails connectionDetails) {
        //todo this is not strictly necessary
        nameToConnectionDetails.put(connectionDetails.getID(), connectionDetails);
        forceRescan();
    }

    /**
     * By default the connections are monitored every 5 seconds If you want to force the map to
     * check immediately use forceRescan
     */
    public void forceRescan() {
        pauser.unpause();
    }


    @Override
    public void eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.BLOCKING;
    }

    @Override
    public void close() throws IOException {

    }

}
