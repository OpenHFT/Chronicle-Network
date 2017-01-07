/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.Pauser;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConnectorEventHandler implements EventHandler, Closeable {

    @NotNull
    private final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier;
    @NotNull
    private final Supplier<SessionDetailsProvider> sessionDetailsSupplier;

    private final Map<String, SocketChannel> descriptionToChannel = new ConcurrentHashMap<>();
    private final Pauser pauser = new LongPauser(0, 0, 5, 5, TimeUnit.SECONDS);
    private final ConnectionStrategy connectionStrategy;
    private EventLoop eventLoop;
    private Map<String, ConnectionDetails> nameToConnectionDetails;

    public ConnectorEventHandler(@NotNull Map<String, ConnectionDetails> nameToConnectionDetails,
                                 @NotNull final Function<ConnectionDetails, TcpHandler> tcpHandlerSupplier,
                                 @NotNull final Supplier<SessionDetailsProvider> sessionDetailsSupplier,
                                 @NotNull final ConnectionStrategy connectionStrategy) {
        this.tcpHandlerSupplier = tcpHandlerSupplier;
        this.sessionDetailsSupplier = sessionDetailsSupplier;
        this.nameToConnectionDetails = nameToConnectionDetails;
        this.connectionStrategy = connectionStrategy;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        nameToConnectionDetails.forEach((k, connectionDetails) -> {
            try {
                SocketChannel socketChannel = descriptionToChannel.get(k);

                if (socketChannel == null) {
                    if (connectionDetails.isDisable()) {
                        //we shouldn't create anything
                        return;
                    }
                    final NetworkStatsListener<NetworkContext> networkStatsListener = connectionDetails.networkStatsListener();
                    SocketAddressSupplier socketAddressSupplier = connectionDetails.sessionProvider();
                    socketChannel = connectionStrategy.connect(k, socketAddressSupplier, networkStatsListener);
                    if (socketChannel == null) {
                        Jvm.warn().on(ConnectorEventHandler.class, "unable to connect to any of the hosts");
                        return;
                    }

                    Jvm.debug().on(ConnectorEventHandler.class, "successfully connect to " + socketAddressSupplier.toString());

                    descriptionToChannel.put(k, socketChannel);
                    connectionDetails.setConnected(true);
                    final SessionDetailsProvider sessionDetails = sessionDetailsSupplier.get();

                    sessionDetails.clientAddress((InetSocketAddress) socketChannel.getRemoteAddress());
                    connectionDetails.socketChannel(socketChannel);
                    @NotNull final TcpEventHandler evntHandler = new TcpEventHandler(connectionDetails);
                    TcpHandler tcpHandler = tcpHandlerSupplier.apply(connectionDetails);
                    evntHandler.tcpHandler(tcpHandler);
                    eventLoop.addHandler(evntHandler);
                } else if (socketChannel.isOpen()) {
                    //the socketChannel is doing fine
                    //check whether it should be disabled
                    if (connectionDetails.isDisable()) {
                        socketChannel.close();
                        connectionDetails.setConnected(false);
                        descriptionToChannel.remove(k);
                    }
                } else {
                    //the socketChannel has disconnected
                    connectionDetails.setConnected(false);
                    descriptionToChannel.remove(k);
                }
            } catch (IOException e) {
                Jvm.warn().on(getClass(), k + e.getMessage());
            }
        });

        pauser.pause();

        return false;
    }

    public void updateConnectionDetails(@NotNull ConnectionDetails connectionDetails) {
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
    public void close() {

    }
}