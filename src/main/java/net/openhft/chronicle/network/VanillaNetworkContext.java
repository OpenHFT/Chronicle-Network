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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.TerminationEventHandler;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class VanillaNetworkContext<T extends VanillaNetworkContext> implements NetworkContext<T> {

    private SocketChannel socketChannel;
    private boolean isAcceptor = true;
    private boolean isUnchecked;
    private HeartbeatListener heartbeatListener;
    private SessionDetailsProvider sessionDetails;
    private boolean connectionClosed;
    private Closeable closeTask;

    @Nullable
    private TerminationEventHandler terminationEventHandler;
    private long heartbeatTimeoutMs;


    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public T socketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        return (T) this;
    }

    /**
     * @param isAcceptor {@code} true if its a server socket, {@code} false if its a client
     * @return
     */
    @Override
    public T isAcceptor(boolean isAcceptor) {
        this.isAcceptor = isAcceptor;
        return (T) this;
    }

    /**
     * @return {@code} true if its a server socket, {@code} false if its a client
     */
    @Override
    public boolean isAcceptor() {
        return isAcceptor;
    }


    @Override
    public boolean isUnchecked() {
        return isUnchecked;
    }

    WireOutPublisher wireOutPublisher;


    @Override
    public synchronized WireOutPublisher wireOutPublisher() {
        return wireOutPublisher;
    }

    @Override
    public void wireOutPublisher(WireOutPublisher wireOutPublisher) {
        this.wireOutPublisher = wireOutPublisher;
    }


    WireType wireType = WireType.TEXT;

    @Override
    public WireType wireType() {
        return wireType;
    }

    public T wireType(WireType wireType) {
        this.wireType = wireType;
        return (T) this;
    }

    @Override
    public SessionDetailsProvider sessionDetails() {
        return this.sessionDetails;
    }

    @Override
    public T sessionDetails(SessionDetailsProvider sessionDetails) {
        this.sessionDetails = sessionDetails;
        return (T) this;
    }

    @Override
    public void closeTask(Closeable closeTask) {
        this.closeTask = closeTask;
    }

    @Override
    public Closeable closeTask() {
        return closeTask;
    }

    public boolean connectionClosed() {
        return this.connectionClosed;
    }

    @Override
    public void connectionClosed(boolean connectionClosed) {
        this.connectionClosed = connectionClosed;
    }

    @Override
    public TerminationEventHandler terminationEventHandler() {
        return terminationEventHandler;
    }

    @Override
    public void terminationEventHandler(@Nullable TerminationEventHandler terminationEventHandler) {
        this.terminationEventHandler = terminationEventHandler;
    }


    public void heartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public long heartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    @Override
    public HeartbeatListener heartbeatListener() {
        return this.heartbeatListener;
    }

    public void heartbeatListener(@NotNull HeartbeatListener heartbeatListener) {
        this.heartbeatListener = heartbeatListener;
    }

}
