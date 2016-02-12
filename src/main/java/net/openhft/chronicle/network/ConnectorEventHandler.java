package net.openhft.chronicle.network;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
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

/**
 * Created by daniel on 09/02/2016.
 * This class handles the creation, running and monitoring of client connections
 */
public class ConnectorEventHandler implements EventHandler, Closeable {

    @NotNull
    private final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier;
    @NotNull
    private final Supplier<SessionDetailsProvider> sessionDetailsSupplier;
    private final long heartbeatIntervalTicks;
    private final long heartbeatTimeOutTicks;
    private final Map<String, SocketChannel> descriptionToChannel = new ConcurrentHashMap<>();
    private final Pauser pauser = new LongPauser(0, 0, 5, 5, TimeUnit.SECONDS);
    private EventLoop eventLoop;
    private Map<String, ConnectionDetails> nameToConnectionDetails;
    private boolean unchecked;

    public ConnectorEventHandler(@NotNull Map<String, ConnectionDetails> nameToConnectionDetails,
                                 @NotNull final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier,
                                 @NotNull final Supplier<SessionDetailsProvider> sessionDetailsSupplier,
                                 long heartbeatIntervalTicks, long heartbeatTimeOutTicks) throws IOException {
        this.nameToConnectionDetails = nameToConnectionDetails;
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        this.sessionDetailsSupplier = sessionDetailsSupplier;
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        this.heartbeatTimeOutTicks = heartbeatTimeOutTicks;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        nameToConnectionDetails.entrySet().forEach(entry -> {
            try {
                SocketChannel socketChannel = descriptionToChannel.get(entry.getKey());

                if (socketChannel == null) {
                    if(entry.getValue().isDisable()){
                        //we shouldn't create anything
                        return;
                    }
                    socketChannel = TCPRegistry.createSocketChannel(entry.getValue().getHostNameDescription());
                    descriptionToChannel.put(entry.getKey(), socketChannel);
                    entry.getValue().setConnected(true);
                    final SessionDetailsProvider sessionDetails = sessionDetailsSupplier.get();

                    sessionDetails.setClientAddress((InetSocketAddress) socketChannel.getRemoteAddress());

                    eventLoop.addHandler(new TcpEventHandler(socketChannel,
                            tcpHandlerSupplier.apply(entry.getValue()),
                            sessionDetails, unchecked,
                            heartbeatIntervalTicks, heartbeatTimeOutTicks));
                }else if (socketChannel.isOpen()) {
                    //the socketChannel is doing fine
                    //check whether it should be disabled
                    if(entry.getValue().isDisable()){
                        socketChannel.close();
                        entry.getValue().setConnected(false);
                        descriptionToChannel.remove(entry.getKey());
                    }
                } else {
                    //the socketChannel has disconnected
                    entry.getValue().setConnected(false);
                    descriptionToChannel.remove(entry.getKey());
                }
            } catch (ConnectException e) {
                //Not a problem try again next time round
                System.out.println(entry.getKey() + e.getMessage());
            } catch (IOException e) {
                System.out.println(entry.getKey() + e.getMessage());
                e.printStackTrace();
            }
        });

        pauser.pause();

        return false;
    }

    public void updateConnectionDetails(ConnectionDetails connectionDetails){
        //todo this is not strictly necessary
        nameToConnectionDetails.put(connectionDetails.getID(), connectionDetails);
        forceRescan();
    }

    /**
     * By default the connections are monitored every 5 seconds
     * If you want to force the map to check immediately use forceRescan
     */
    public void forceRescan(){
        pauser.unpause();
    }

    public void unchecked(boolean unchecked) {
        this.unchecked = unchecked;
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
