package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Created by daniel on 09/02/2016.
 */
public class ConnectorEventHandler implements EventHandler, Closeable {

    private EventLoop eventLoop;
    private Map<String, ConnectionDetails> nameToConnectionDetails;
    @NotNull
    private final Supplier<TcpHandler> tcpHandlerSupplier;
    @NotNull
    private final Supplier<SessionDetailsProvider> sessionDetailsSupplier;
    private final long heartbeatIntervalTicks;
    private final long heartbeatTimeOutTicks;
    private boolean unchecked;
    private final Map<String, SocketChannel> descriptionToChannel = new ConcurrentHashMap<>();

    public ConnectorEventHandler(@NotNull Map<String, ConnectionDetails> nameToConnectionDetails,
                                 @NotNull final Supplier<TcpHandler> tcpHandlerSupplier,
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
                            tcpHandlerSupplier.get(),
                            sessionDetails, unchecked,
                            heartbeatIntervalTicks, heartbeatTimeOutTicks));
                }else if(!socketChannel.isConnected()){
                    //the socketChannel has disconnected
                    entry.getValue().setConnected(false);
                    descriptionToChannel.remove(entry.getKey());
                }else{
                    //the socketChannel is doing fine
                    //check whether it should be disabled
                    if(entry.getValue().isDisable()){
                        socketChannel.close();
                        entry.getValue().setConnected(false);
                        descriptionToChannel.remove(entry.getKey());
                    }
                }
            } catch (ConnectException e) {
                //todo add a timer
                //Not a problem try again next time round
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


        return false;
    }

    public void updateConnectionDetails(ConnectionDetails connectionDetails){
        nameToConnectionDetails.put(connectionDetails.getName(), connectionDetails);
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

    public static class ConnectionDetails {
        private boolean isConnected;
        private String name;
        private String hostNameDescription;
        private boolean disable;

        public ConnectionDetails(String name, String hostNameDescription) {
            this.name = name;
            this.hostNameDescription = hostNameDescription;
        }

        public String getName() {
            return name;
        }

        boolean isConnected() {
            return isConnected;
        }

        public String getHostNameDescription() {
            return hostNameDescription;
        }

        public void setHostNameDescription(String hostNameDescription) {
            this.hostNameDescription = hostNameDescription;
        }

        void setConnected(boolean connected) {
            isConnected = connected;
        }

        public boolean isDisable() {
            return disable;
        }

        public void setDisable(boolean disable) {
            this.disable = disable;
        }
    }
}
