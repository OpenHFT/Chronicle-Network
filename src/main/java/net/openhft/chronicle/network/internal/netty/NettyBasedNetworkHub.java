/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.internal.netty;

import io.netty.channel.ChannelException;
import io.netty.channel.EventLoopException;
import io.netty.channel.nio.NioTask;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import net.openhft.chronicle.network.NioCallback;
import net.openhft.chronicle.network.NioCallback.EventType;
import net.openhft.chronicle.network.NioCallbackFactory;
import net.openhft.chronicle.network.internal.*;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Work in progress -  an NIO API abstraction
 *
 * @author Rob Austin.
 */
public final class NettyBasedNetworkHub<T> extends AbstractNetwork implements Closeable {

    @NotNull
    private final NioCallbackFactory nioCallbackFactory;

    private final int defaultBufferSize;
    private static final Logger LOG = LoggerFactory.getLogger(NettyBasedNetworkHub.class.getName());


    public static final long SPIN_LOOP_TIME_IN_NONOSECONDS = TimeUnit.MICROSECONDS.toNanos(500);

    private final OpWriteInterestUpdater opWriteUpdater = new OpWriteInterestUpdater();

    private final long heartBeatIntervalMillis;
    private long largestEntrySoFar = 128;


    @NotNull
    private final NetworkConfig replicationConfig;

    private final String name;
    private long selectorTimeout;


    /**
     * @throws java.io.IOException on an io error.
     */
    public NettyBasedNetworkHub(@NotNull final NetworkConfig replicationConfig,
                                @NotNull final NioCallbackFactory nioCallbackFactory)
            throws IOException {

        super("TcpReplicator-" + replicationConfig.name(), replicationConfig.throttlingConfig());
        this.nioCallbackFactory = nioCallbackFactory;


        final ThrottlingConfig throttlingConfig = replicationConfig.throttlingConfig();
        long throttleBucketInterval = throttlingConfig.bucketInterval(MILLISECONDS);

        heartBeatIntervalMillis = replicationConfig.heartBeatInterval(MILLISECONDS);

        selectorTimeout = Math.min(heartBeatIntervalMillis / 4, throttleBucketInterval);

        this.replicationConfig = replicationConfig;
        defaultBufferSize = replicationConfig.tcpBufferSize();

        this.name = replicationConfig.name();

        selector = openSelector();
        start();
    }

    @Override
    protected void processEvent() throws IOException {
        try {
            final InetSocketAddress serverInetSocketAddress = replicationConfig
                    .inetSocketAddress();

            final Details serverDetails = new Details(serverInetSocketAddress);
            new ServerConnector(serverDetails).connect();

            for (InetSocketAddress client : replicationConfig.endpoints()) {
                final Details clientDetails = new Details(client);
                new ClientConnector(clientDetails).connect();
            }


            while (selector.isOpen()) {
                if (hasPendingRegistrations.getAndSet(false))
                    registerPendingRegistrations();

                // set the WRITE when data is ready to send
                opWriteUpdater.applyUpdates(selector);

                boolean oldWakenUp = wakenUp.getAndSet(false);
                //  try {
                if (opWriteUpdater.wasChanged()) {
                    selectNow();
                } else {
                    select(oldWakenUp);

                    // 'wakenUp.compareAndSet(false, true)' is always evaluated
                    // before calling 'selector.wakeup()' to reduce the wake-up
                    // overhead. (Selector.wakeup() is an expensive operation.)
                    //
                    // However, there is a race condition in this approach.
                    // The race condition is triggered when 'wakenUp' is set to
                    // true too early.
                    //
                    // 'wakenUp' is set to true too early if:
                    // 1) Selector is waken up between 'wakenUp.set(false)' and
                    //    'selector.select(...)'. (BAD)
                    // 2) Selector is waken up between 'selector.select(...)' and
                    //    'if (wakenUp.get()) { ... }'. (OK)
                    //
                    // In the first case, 'wakenUp' is set to true and the
                    // following 'selector.select(...)' will wake up immediately.
                    // Until 'wakenUp' is set to false again in the next round,
                    // 'wakenUp.compareAndSet(false, true)' will fail, and therefore
                    // any attempt to wake up the Selector will fail, too, causing
                    // the following 'selector.select(...)' call to block
                    // unnecessarily.
                    //
                    // To fix this problem, we wake up the selector again if wakenUp
                    // is true immediately after selector.select(...).
                    // It is inefficient in that it wakes up the selector for both
                    // the first case (BAD - wake-up required) and the second case
                    // (OK - no wake-up required).

                    if (wakenUp.get()) {
                        selector.wakeup();
                    }
                }

                cancelledKeys = 0;
                needsToSelectAgain = false;
                final int ioRatio = this.ioRatio;


                processSelectedKeys();



               /* } catch (Throwable t) {
                    logger.warn("Unexpected exception in the selector loop.", t);

                    // Prevent possible consecutive immediate failures that lead to
                    // excessive CPU consumption.
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // Ignore.
                    }
                }*/
            }


        } catch (CancelledKeyException | ClosedChannelException |
                ClosedSelectorException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (Exception e) {
            LOG.error("", e);
        } catch (Throwable e) {
            LOG.error("", e);
            throw e;
        } finally {

            if (LOG.isDebugEnabled())
                LOG.debug("closing name=" + name);
            if (!isClosed) {
                closeResources();
            }
        }
    }

    private void processKey(long approxTime, @NotNull SelectionKey key) {
        try {

            if (!key.isValid())
                return;

            if (key.isAcceptable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onAccept - " + name);
                onAccept(key);
            }

            if (key.isConnectable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onConnect - " + name);
                onConnect(key);
            }

            if (key.isReadable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onRead - " + name);
                onRead(key, approxTime);
            }
            if (key.isWritable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onWrite - " + name);
                onWrite(key, approxTime);
            }
        } catch (BufferUnderflowException | InterruptedException | IOException |
                ClosedSelectorException | CancelledKeyException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (Exception e) {
            LOG.info("", e);
            if (!isClosed)
                closeEarlyAndQuietly(key);
        }
    }

    /**
     * spin loops 100000 times first before calling the selector with timeout
     *
     * @return The number of keys, possibly zero, whose ready-operation sets were updated
     * @throws java.io.IOException
     */
    private int select() throws IOException {

        long start = System.nanoTime();

        while (System.nanoTime() < start + SPIN_LOOP_TIME_IN_NONOSECONDS) {
            final int keys = selector.selectNow();
            if (keys != 0)
                return keys;
        }

        return selector.select(selectorTimeout);
    }

    /**
     * checks that we receive heartbeats and send out heart beats.
     *
     * @param approxTime the approximate time in milliseconds
     */

    void heartBeatMonitor(long approxTime) {
        for (SelectionKey key : selector.keys()) {
            try {

                if (!key.isValid() || !key.channel().isOpen()) {
                    continue;
                }

                final Attached attachment = (Attached) key.attachment();

                if (attachment == null)
                    continue;

                if (!attachment.hasRemoteHeartbeatInterval)
                    continue;

                try {
                    sendHeartbeatIfRequired(approxTime, key);
                } catch (Exception e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }

                try {
                    heartbeatCheckHasReceived(key, approxTime);
                } catch (Exception e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }
            } catch (Exception e) {
                if (LOG.isDebugEnabled())
                    LOG.debug("", e);
            }
        }
    }

    /**
     * check to see if its time to send a heartbeat, and send one if required
     *
     * @param approxTime the current time ( approximately )
     * @param key        nio selection key
     */
    private void sendHeartbeatIfRequired(final long approxTime,
                                         @NotNull final SelectionKey key) {
        final Attached attachment = (Attached) key.attachment();

        if (attachment.sendHeartbeat && attachment.writer
                .lastSentTime +
                heartBeatIntervalMillis < approxTime) {
            attachment.writer.lastSentTime = approxTime;
            attachment.writer.writeHeartbeatToBuffer();

            enableOpWrite(key);

            if (LOG.isDebugEnabled())
                LOG.debug("sending heartbeat");
        }
    }

    private static void enableOpWrite(@NotNull SelectionKey key) {
        int ops = key.interestOps();
        key.interestOps(ops | OP_WRITE);
    }


    private void enableOpRead(@NotNull SelectionKey key) {
        int ops = key.interestOps();
        key.interestOps(ops | OP_READ);
    }


    /**
     * check to see if we have lost connection with the remote node and if we have attempts a
     * reconnect.
     *
     * @param key               the key relating to the heartbeat that we are checking
     * @param approxTimeOutTime the approximate time in milliseconds
     * @throws java.net.ConnectException
     */
    private void heartbeatCheckHasReceived(@NotNull final SelectionKey key,
                                           final long approxTimeOutTime) {

        final Attached attached = (Attached) key.attachment();

        // we wont attempt to reconnect the server socket
        if (attached.isServer || !attached.receivesHeartbeat)
            return;

        final SocketChannel channel = (SocketChannel) key.channel();

        if (approxTimeOutTime >
                attached.reader.lastHeartBeatReceived + attached.remoteHeartbeatInterval) {
            if (LOG.isDebugEnabled())
                LOG.debug("lost connection, attempting to reconnect. " +
                        "missed heartbeat from identifier=" + attached.remoteIdentifier);

            closeables.closeQuietly(channel.socket());

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes, so these nodes
            // will establish the connection when they come back up, hence under these
            // circumstances, polling a dropped node to attempt to reconnect is no-longer
            // required as the remote node will establish the connection its self on startup.
            if (replicationConfig.autoReconnectedUponDroppedConnection())
                attached.connector.connectLater();
        }
    }

    /**
     * closes and only logs the exception at debug
     *
     * @param key the SelectionKey
     * @param e   the Exception that caused the issue
     */
    private void quietClose(@NotNull final SelectionKey key, @NotNull final Exception e) {
        if (LOG.isDebugEnabled())
            LOG.debug("", e);
        closeEarlyAndQuietly(key);
    }

    /**
     * called when the selector receives a OP_CONNECT message
     */
    private void onConnect(@NotNull final SelectionKey key)
            throws IOException {

        SocketChannel channel = null;

        try {
            channel = (SocketChannel) key.channel();
        } finally {
            closeables.add(channel);
        }

        final Attached attached = (Attached) key.attachment();

        try {
            if (!channel.finishConnect())
                return;

        } catch (SocketException e) {

            quietClose(key, e);

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes,
            // so these nodes will establish the connection when they come back up,
            // hence under these circumstances, polling a dropped node to attempt to reconnect
            // is no-longer required as the remote node will establish the connection its self
            // on startup.

            attached.connector.connect();

            throw e;
        }

        attached.connector.setSuccessfullyConnected();

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        attached.reader = new Reader(defaultBufferSize, name);
        attached.writer = new Writer(defaultBufferSize);


        throttle(channel);

        NioCallback userAttached = nioCallbackFactory.onCreate(attached);
        attached.setUserAttached(userAttached);


        onEvent(key, attached, EventType.CONNECT);
        enableOpRead(key);

    }

    /**
     * called when the selector receives a OP_ACCEPT message
     */
    private void onAccept(@NotNull final SelectionKey key) throws IOException {
        ServerSocketChannel server = null;

        try {
            server = (ServerSocketChannel) key.channel();
        } finally {
            if (server != null)
                closeables.add(server);
        }

        SocketChannel channel = null;

        assert server != null;

        try {
            channel = server.accept();
        } finally {
            if (channel != null)
                closeables.add(channel);
        }

        assert channel != null;

        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);


        final Attached attached = new Attached(opWriteUpdater, heartBeatIntervalMillis, this);
        attached.reader = new Reader(defaultBufferSize, name);
        attached.writer = new Writer(defaultBufferSize);
        attached.isServer = true;

        NioCallback userAttached = nioCallbackFactory.onCreate(attached);
        attached.setUserAttached(userAttached);


        Bytes writer = attached.writer.in();
        long start = writer.position();

        attached.getUserAttached().onEvent(attached.reader.out, writer,
                EventType.ACCEPT);

        if (attached.writer.in().position() > start)

            channel.register(selector, OP_READ | OP_WRITE, attached);
        else
            channel.register(selector, OP_READ, attached);


        throttle(channel);
    }


    /**
     * called when the selector receives a WRITE message
     */
    private void onWrite(@NotNull final SelectionKey key,
                         final long approxTime) throws IOException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();
        if (attached == null) {
            LOG.info("Closing connection " + socketChannel + ", nothing attached");
            socketChannel.close();
            return;
        }

        if (attached.writer.in().remaining() > 0)
            attached.getUserAttached().onEvent(attached.reader.out, attached.writer.in(), EventType.WRITE);


        Writer writer = attached.writer;

        try {
            final int len = writer.writeBufferToSocket(socketChannel, approxTime);

            if (len == -1)
                socketChannel.close();

            //  if (len > 0)
            //      contemplateThrottleWrites(len);

            if (writer.in.position() == 0) {

                // TURN OP_WRITE_OFF
                key.interestOps(key.interestOps() & ~OP_WRITE);
            }


        } catch (IOException e) {
            quietClose(key, e);
            if (!attached.isServer)
                attached.connector.connectLater();
            throw e;
        }
    }

    /**
     * called when the selector receives a OP_READ message
     */

    private void onRead(@NotNull final SelectionKey key,
                        final long approxTime) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached == null) {
            LOG.info("Closing connection " + socketChannel + ", nothing attached");
            socketChannel.close();
            return;
        }

        try {

            int len = attached.reader.readSocketToBuffer(socketChannel, largestEntrySoFar);

            if (len == -1) {
                socketChannel.register(selector, 0);
                if (replicationConfig.autoReconnectedUponDroppedConnection()) {
                    AbstractConnector connector = attached.connector;
                    if (connector != null)
                        connector.connectLater();
                } else
                    socketChannel.close();
                return;
            }

            if (attached.reader.out.remaining() > 0) {
                onEvent(key, attached, EventType.READ);
            }

            if (len == 0)
                return;

        } catch (IOException e) {
            if (!attached.isServer)
                attached.connector.connectLater();
            throw e;
        }

        if (LOG.isDebugEnabled())
            LOG.debug("heartbeat or data received.");

        attached.reader.lastHeartBeatReceived = approxTime;


    }

    private static void onEvent(SelectionKey key, Attached attached, final EventType type) {


        long start = attached.writer.in().position();
        attached.getUserAttached().onEvent(attached.reader.out, attached.writer.in(), type);

        if (attached.writer.in().position() > start) {
            enableOpWrite(key);
        }
    }

    @Nullable
    private ServerSocketChannel openServerSocketChannel() throws IOException {
        ServerSocketChannel result = null;

        try {
            result = ServerSocketChannel.open();
        } finally {
            if (result != null)
                closeables.add(result);
        }
        return result;
    }

    /**
     * sets interestOps to "selector keys",The change to interestOps much be on the same thread as
     * the selector. This class, allows via {@link net.openhft.chronicle.network.internal.AbstractNetwork
     * .KeyInterestUpdater#set(int)}  to holds a pending change  in interestOps ( via a bitset ),
     * this change is processed later on the same thread as the selector
     */
    private static class OpWriteInterestUpdater {

        private final AtomicBoolean wasChanged = new AtomicBoolean();


        public void applyUpdates(final Selector selector1) {

            if (wasChanged.getAndSet(false)) {

                for (SelectionKey selectionKey : selector1.keys()) {

                    Attached attached = (Attached) selectionKey.attachment();
                    if (attached.isDirty()) {
                        selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);
                    }

                }
            }

        }

        public void onChange() {
            wasChanged.set(true);
        }

        public boolean wasChanged() {
            return wasChanged.get();
        }

    }

    class ServerConnector extends AbstractConnector {

        @NotNull
        private final Details details;

        private ServerConnector(@NotNull Details details) {
            super("TCP-ServerConnector-" + details);
            this.details = details;
        }

        @NotNull
        @Override
        public String toString() {
            return "ServerConnector{" +
                    "" + details +
                    '}';
        }

        @Nullable
        protected SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = openServerSocketChannel();
            //   serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);

            //  serverChannel.register(NetworkHub.this.selector, 0);
            ServerSocket serverSocket = null;

            try {
                serverSocket = serverChannel.socket();
            } finally {
                if (serverSocket != null)
                    closeables.add(serverSocket);
            }

            serverSocket.setReuseAddress(true);
            serverSocket.setSoTimeout(0);
            serverSocket.bind(details.address());

            // these can be run on this thread
            addPendingRegistration(new Runnable() {
                @Override
                public void run() {
                    final Attached attached = new Attached(opWriteUpdater,
                            heartBeatIntervalMillis, NettyBasedNetworkHub.this);
                    attached.connector = ServerConnector.this;
                    try {
                        serverChannel.register(NettyBasedNetworkHub.this.selector, OP_ACCEPT, attached);
                    } catch (ClosedChannelException e) {
                        LOG.debug("", e);
                    }
                }
            });

            selector.wakeup();

            return serverChannel;
        }
    }

    private class ClientConnector extends AbstractConnector {

        @NotNull
        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super("TCP-ClientConnector-" + details);
            this.details = details;
        }

        @NotNull
        @Override
        public String toString() {
            return "ClientConnector{" + details + '}';
        }

        /**
         * blocks until connected
         */
        @Override
        protected SelectableChannel doConnect() throws IOException, InterruptedException {
            boolean success = false;

            final SocketChannel socketChannel = openSocketChannel(NettyBasedNetworkHub.this.closeables);

            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setReuseAddress(true);
                socketChannel.socket().setSoLinger(false, 0);
                socketChannel.socket().setSoTimeout(0);
                socketChannel.socket().setTcpNoDelay(true);

                try {
                    socketChannel.connect(details.address());
                } catch (UnresolvedAddressException e) {
                    this.connectLater();
                }

                // Under experiment, the concoction was found to be more successful if we
                // paused before registering the OP_CONNECT
                Thread.sleep(10);

                // the registration has be be run on the same thread as the selector
                addPendingRegistration(new Runnable() {
                    @Override
                    public void run() {
                        final Attached attached = new Attached(opWriteUpdater,
                                heartBeatIntervalMillis, NettyBasedNetworkHub.this);
                        attached.connector = ClientConnector.this;

                        try {
                            socketChannel.register(selector, OP_CONNECT, attached);
                        } catch (ClosedChannelException e) {
                            if (socketChannel.isOpen())
                                LOG.error("", e);
                        }
                    }
                });

                selector.wakeup();
                success = true;
                return socketChannel;
            } finally {
                if (!success) {
                    try {
                        try {
                            socketChannel.socket().close();
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                        socketChannel.close();
                    } catch (IOException e) {
                        LOG.error("", e);
                    }
                }
            }
        }
    }

    /**
     * Attached to the NIO selection key via methods such as {@link java.nio.channels.SelectionKey#attach(Object)}
     */
    public static class Attached<T> implements Actions, NioCallbackProvider {


        private final OpWriteInterestUpdater opWriteUpdater;
        private final long heartBeatIntervalMillis;

        public long remoteHeartbeatInterval;
        private final Closeable closeable;

        public Attached(OpWriteInterestUpdater opWriteUpdater, long heartBeatIntervalMillis,
                        Closeable closeable) {
            this.heartBeatIntervalMillis = heartBeatIntervalMillis;
            this.opWriteUpdater = opWriteUpdater;
            remoteHeartbeatInterval = heartBeatIntervalMillis;
            this.closeable = closeable;
        }

        public Reader reader;
        public Writer writer;

        private NioCallback userAttached;

        private volatile boolean isDirty;
        private boolean receivesHeartbeat;
        private boolean sendHeartbeat;


        @Nullable
        public NioCallback getUserAttached() {
            return userAttached;
        }

        public void setUserAttached(@Nullable NioCallback userAttached) {
            this.userAttached = userAttached;
        }

        public AbstractConnector connector;

        public byte remoteIdentifier = Byte.MIN_VALUE;
        public boolean hasRemoteHeartbeatInterval;

        // true if its socket is a ServerSocket
        public boolean isServer;


        @Override
        public void setDirty(boolean isDirty) {
            this.isDirty = isDirty;
            opWriteUpdater.onChange();
        }


        @Override
        public void setReceiveHeartbeat(boolean receivesHeartbeat) {
            this.receivesHeartbeat = receivesHeartbeat;
        }

        @Override
        public void setSendHeartbeat(boolean sendHeartbeat) {
            this.sendHeartbeat = sendHeartbeat;
        }

        @Override
        public void close() {

            try {

                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
            }
        }

        @Override
        public Bytes outWithSize(int size) {

            long additional = size - writer.in().remaining();
            assert additional <= Integer.MAX_VALUE;
            return (additional > 0) ? writer.increaseBufferBy((int) additional) : writer.in();

        }

        boolean isDirty() {
            return isDirty;
        }
    }

    /**
     * @author Rob Austin.
     */
    static class Writer {

        @NotNull
        private ByteBufferBytes in;

        @NotNull
        private ByteBuffer out;

        private long lastSentTime;

        private Writer(final int tcpBufferSize1) {
            out = ByteBuffer.allocateDirect(tcpBufferSize1);
            in = new ByteBufferBytes(out);
        }

        private Bytes resizeBuffer(int size) {

            if (LOG.isDebugEnabled())
                LOG.debug("resizing buffer to size=" + size);

            if (size < out.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            assert size < Integer.MAX_VALUE;

            final ByteBuffer result = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
            final long bytesPosition = in.position();

            in = new ByteBufferBytes(result);

            out.position(0);
            out.limit((int) bytesPosition);
            in.write(out);
            out = result;

            assert out.capacity() == in.capacity();

            assert out.capacity() == size;
            assert out.capacity() == in.capacity();
            assert in.limit() == in.capacity();
            return in;
        }

        void ensureBufferSize(long size) {
            if (in().remaining() < size) {
                size += in().position();
                if (size > Integer.MAX_VALUE)
                    throw new UnsupportedOperationException();
                resizeBuffer((int) size);
            }
        }

        void resizeToMessage(@NotNull IllegalStateException e) {

            String message = e.getMessage();
            if (message.startsWith("java.io.IOException: Not enough available space for writing ")) {
                String substring = message.substring("java.io.IOException: Not enough available space for writing ".length(), message.length());
                int i = substring.indexOf(' ');
                if (i != -1) {
                    int size = Integer.parseInt(substring.substring(0, i));

                    long requiresExtra = size - in().remaining();
                    ensureBufferSize((int) (in().capacity() + requiresExtra));
                } else
                    throw e;
            } else
                throw e;
        }

        Bytes in() {
            return in;
        }

        private ByteBuffer out() {
            return out;
        }


        /**
         * writes the contents of the buffer to the socket
         *
         * @param socketChannel the socket to publish the buffer to
         * @param approxTime    an approximation of the current time in millis
         * @throws java.io.IOException
         */
        private int writeBufferToSocket(@NotNull final SocketChannel socketChannel,
                                        final long approxTime) throws IOException {

            final Bytes in = in();
            final ByteBuffer out = out();

            if (out.limit() == 0)
                return 0;

            // if we still have some unwritten writer from last time
            lastSentTime = approxTime;
            assert in.position() <= Integer.MAX_VALUE;
            int expectedSize = (int) in.position();

            out.limit(expectedSize);
            out.position(0);

            final int len = socketChannel.write(out);

            if (LOG.isDebugEnabled())
                LOG.debug("bytes-written=" + len);

            if (len == expectedSize) {
                out.clear();
                in.clear();
            } else {
                out.compact();
                in.position(out.position());
                in.limit(in.capacity());
                out.limit(out.capacity());
            }


            return len;
        }


        /**
         * used to send an single zero byte if we have not send any data for up to the
         * localHeartbeatInterval
         */
        private void writeHeartbeatToBuffer() {

            // denotes the state - 0 for a heartbeat
            in().writeByte(0);

            // denotes the size in bytes
            in().writeInt(0);
        }

        /**
         * removes back in the WRITE from the selector, otherwise it'll spin loop. The WRITE will
         * get added back in as soon as we have data to write
         *
         * @param socketChannel the socketChannel we wish to stop writing to
         * @param selector
         */
        public synchronized void disableWrite(@NotNull final SocketChannel socketChannel,
                                              final Selector selector) {
            try {
                SelectionKey key = socketChannel.keyFor(selector);
                if (key != null) {
                    if (selector.isOpen()) {
                        key.interestOps(key.interestOps() & ~OP_WRITE);
                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            }
        }


        public Bytes increaseBufferBy(int additionalBytes) {
            resizeBuffer(out.capacity() + additionalBytes);
            return in();
        }
    }

    /**
     * Reads map entries from a socket, this could be a client or server socket
     */
    static class Reader {

        private final String name;
        public long lastHeartBeatReceived = System.currentTimeMillis();
        ByteBuffer in;
        ByteBufferBytes out;


        @Override
        public String toString() {
            return "TcpSocketChannelEntryReader{" +
                    "name='" + name + '\'' +
                    ", lastHeartBeatReceived=" + lastHeartBeatReceived +
                    ", in=" + in +
                    ", out=" + out +
                    '}';
        }

        private Reader(final int defaultBufferSize, final String name) {
            this.name = name;


            in = ByteBuffer.allocateDirect(defaultBufferSize);
            out = new ByteBufferBytes(in.slice());
            out.limit(0);
            in.clear();
        }

        void resizeBuffer(long size) {
            assert size < Integer.MAX_VALUE;

            if (size < in.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            final ByteBuffer buffer = ByteBuffer.allocateDirect((int) size)
                    .order(ByteOrder.nativeOrder());

            final int inPosition = in.position();

            long outPosition = out.position();
            long outLimit = out.limit();

            out = new ByteBufferBytes(buffer.slice());

            in.flip();
            buffer.put(in);

            in = buffer;
            in.limit(in.capacity());
            in.position(inPosition);

            out.limit(outLimit);
            out.position(outPosition);
        }

        /**
         * reads from the socket and writes them to the buffer
         *
         * @param socketChannel     the  socketChannel to read from
         * @param largestEntrySoFar
         * @return the number of bytes read
         * @throws java.io.IOException
         */
        private int readSocketToBuffer(@NotNull final SocketChannel socketChannel,
                                       final long largestEntrySoFar)
                throws IOException {

//            assert out.limit() != out.capacity();
            compactBuffer(largestEntrySoFar);
            //   in.position((int) out.limit());

            final int len = socketChannel.read(in);
            out.limit(in.position());
            return len;
        }


        /**
         * compacts the buffer and updates the {@code in} and {@code out} accordingly
         *
         * @param largestEntrySoFar
         */
        private void compactBuffer(final long largestEntrySoFar) {
            // the maxEntrySizeBytes used here may not be the maximum size of the entry in its
            // serialized form however, its only use as an indication that the buffer is becoming
            // full and should be compacted the buffer can be compacted at any time
            if (in.position() == 0)
                return;


            if (out.remaining() == 0) {
                out.position(0);
                out.limit(0);
                in.position(0);
            } else {

                if (in.capacity() - out.limit() > (largestEntrySoFar * 2))
                    // its not necessary to compact the buffer
                    return;

                // compact the buffer
                in.limit((int) out.capacity());
                in.position((int) out.position());
                in.compact();

                out.position(0);
                out.limit((int) in.position());


            }


           /* if (out.position() == in.position()) {
                out.position(0);
                in.position(0);
            } else {
                in.limit(in.position());
                assert out.position() < Integer.MAX_VALUE;
                in.position((int) out.position());

                in.compact();
                out.position(0);
            }*/

        }


    }

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioEventLoop.class);

    private static final int CLEANUP_INTERVAL = 256; // XXX Hard-coded value, but won't need customization.

    private static final boolean DISABLE_KEYSET_OPTIMIZATION =
            SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);

    private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
    private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

    // Workaround for JDK NIO bug.
    //
    // See:
    // - http://bugs.sun.com/view_bug.do?bug_id=6427854
    // - https://github.com/netty/netty/issues/203
    static {
        String key = "sun.nio.ch.bugLevel";
        try {
            String buglevel = SystemPropertyUtil.get(key);
            if (buglevel == null) {
                System.setProperty(key, "");
            }
        } catch (SecurityException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to get/set System Property: {}", key, e);
            }
        }

        int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
        if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
            selectorAutoRebuildThreshold = 0;
        }

        SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;

        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEYSET_OPTIMIZATION);
            logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
        }
    }

    /**
     * The NIO {@link Selector}.
     */

    private SelectedSelectionKeySet selectedKeys;

    private final SelectorProvider provider = SelectorProvider.provider();

    /**
     * Boolean that controls determines if a blocked Selector.select should break out of its
     * selection process. In our case we use a timeout for the select method and the select method
     * will block for that time unless waken up.
     */
    private final AtomicBoolean wakenUp = new AtomicBoolean();

    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;


    private Selector openSelector() {
        final Selector selector;
        try {
            selector = provider.openSelector();
        } catch (IOException e) {
            throw new ChannelException("failed to open a new selector", e);
        }

        if (DISABLE_KEYSET_OPTIMIZATION) {
            return selector;
        }

        try {
            SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

            Class<?> selectorImplClass =
                    Class.forName("sun.nio.ch.SelectorImpl", false, PlatformDependent.getSystemClassLoader());

            // Ensure the current selector implementation is what we can instrument.
            if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
                return selector;
            }

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

            selectedKeysField.setAccessible(true);
            publicSelectedKeysField.setAccessible(true);

            selectedKeysField.set(selector, selectedKeySet);
            publicSelectedKeysField.set(selector, selectedKeySet);

            selectedKeys = selectedKeySet;
            logger.trace("Instrumented an optimized java.util.Set into: {}", selector);
        } catch (Throwable t) {
            selectedKeys = null;
            logger.trace("Failed to instrument an optimized java.util.Set into: {}", selector, t);
        }

        return selector;
    }


    /**
     * Registers an arbitrary {@link SelectableChannel}, not necessarily created by Netty, to the
     * {@link Selector} of this event loop.  Once the specified {@link SelectableChannel} is
     * registered, the specified {@code task} will be executed by this event loop when the {@link
     * SelectableChannel} is ready.
     */
    public void register(final SelectableChannel ch, final int interestOps, final NioTask<?> task) {
        if (ch == null) {
            throw new NullPointerException("ch");
        }
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }
        if ((interestOps & ~ch.validOps()) != 0) {
            throw new IllegalArgumentException(
                    "invalid interestOps: " + interestOps + "(validOps: " + ch.validOps() + ')');
        }
        if (task == null) {
            throw new NullPointerException("task");
        }


        try {
            ch.register(selector, interestOps, task);
        } catch (Exception e) {
            throw new EventLoopException("failed to register a channel", e);
        }
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop.  The
     * default value is {@code 50}, which means the event loop will try to spend the same amount of
     * time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }


    private void processSelectedKeys() {


        SelectionKey[] selectedKeys1 = selectedKeys.flip();
        for (int i = 0; ; i++) {
            final SelectionKey k = selectedKeys1[i];
            if (k == null) {
                break;
            }
            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363
            selectedKeys1[i] = null;

            processSelectedKey(k);

            if (needsToSelectAgain) {
                // null out entries in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                for (; ; ) {
                    if (selectedKeys1[i] == null) {
                        break;
                    }
                    selectedKeys1[i] = null;
                    i++;
                }

                selectAgain();
                // Need to flip the optimized selectedKeys to get the right reference to the array
                // and reset the index to -1 which will then set to 0 on the for loop
                // to start over again.
                //
                // See https://github.com/netty/netty/issues/1523
                selectedKeys1 = this.selectedKeys.flip();
                i = -1;
            }
        }

    }


    private void processSelectedKey(SelectionKey k) {
        int state = 0;
        try {
            processKey(System.currentTimeMillis(), k);
            state = 1;
        } catch (Exception e) {
            k.cancel();
            state = 2;
        } finally {

            switch (state) {
                case 0:
                    k.cancel();
                    break;

            }
        }
    }


    void cancel(SelectionKey key) {
        key.cancel();
        cancelledKeys++;
        if (cancelledKeys >= CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            needsToSelectAgain = true;
        }
    }


    private void processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
        // check if the set is empty and if so just return to not create garbage by
        // creating a new Iterator every time even if there is nothing to process.
        // See https://github.com/netty/netty/issues/597
        if (selectedKeys.isEmpty()) {
            return;
        }

        Iterator<SelectionKey> i = selectedKeys.iterator();
        for (; ; ) {
            final SelectionKey k = i.next();
            final Object a = k.attachment();
            i.remove();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (!i.hasNext()) {
                break;
            }

            if (needsToSelectAgain) {
                selectAgain();
                selectedKeys = selector.selectedKeys();

                // Create the iterator again to avoid ConcurrentModificationException
                if (selectedKeys.isEmpty()) {
                    break;
                } else {
                    i = selectedKeys.iterator();
                }
            }
        }
    }


    private static void processSelectedKey(SelectionKey k, AbstractNioChannel ch) {
        final AbstractNioChannel.NioUnsafe unsafe = ch.unsafe();
        if (!k.isValid()) {
            // close the channel if the key is not valid anymore
            unsafe.close(unsafe.voidPromise());
            return;
        }

        try {
            int readyOps = k.readyOps();
            // Also check for readOps of 0 to workaround possible JDK bug which may otherwise lead
            // to a spin loop
            if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                unsafe.read();
                if (!ch.isOpen()) {
                    // Connection already closed - no need to handle write.
                    return;
                }
            }
            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                // Call forceFlush which will also take care of clear the WRITE once there is nothing left to write
                ch.unsafe().forceFlush();
            }
            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {

                // remove OP_CONNECT as otherwise Selector.select(..) will always return without blocking
                // See https://github.com/netty/netty/issues/924
                int ops = k.interestOps();
                ops &= ~SelectionKey.OP_CONNECT;
                k.interestOps(ops);

                unsafe.finishConnect();
            }
        } catch (CancelledKeyException ignored) {
            unsafe.close(unsafe.voidPromise());
        }
    }

    private static void processSelectedKey(SelectionKey k, NioTask<SelectableChannel> task) {
        int state = 0;
        try {
            task.channelReady(k.channel(), k);
            state = 1;
        } catch (Exception e) {
            k.cancel();
            invokeChannelUnregistered(task, k, e);
            state = 2;
        } finally {
            switch (state) {
                case 0:
                    k.cancel();
                    invokeChannelUnregistered(task, k, null);
                    break;
                case 1:
                    if (!k.isValid()) { // Cancelled by channelReady()
                        invokeChannelUnregistered(task, k, null);
                    }
                    break;
            }
        }
    }

    @Override
    public void close() {
        try {
            selector.close();
        } catch (IOException e) {
            LOG.error("", e);
        }
        super.close();
    }


    private static void invokeChannelUnregistered(NioTask<SelectableChannel> task, SelectionKey k, Throwable cause) {
        try {
            task.channelUnregistered(k.channel(), cause);
        } catch (Exception e) {
            logger.warn("Unexpected exception while running NioTask.channelUnregistered()", e);
        }
    }


    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && wakenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    void selectNow() throws IOException {
        try {
            selector.selectNow();
        } finally {
            // restore wakup state if needed
            if (wakenUp.get()) {
                selector.wakeup();
            }
        }
    }

    private void select(boolean oldWakenUp) throws IOException {
        Selector selector = this.selector;
        try {
            int selectCnt = 0;
            long currentTimeNanos = System.nanoTime();
            long selectDeadLineNanos = currentTimeNanos + 10000;
            for (; ; ) {
                long timeoutMillis = (selectDeadLineNanos - currentTimeNanos + 500000L) / 1000000L;
                if (timeoutMillis <= 0) {
                    if (selectCnt == 0) {
                        selector.selectNow();
                        selectCnt = 1;
                    }
                    break;
                }

                int selectedKeys = selector.select(timeoutMillis);
                selectCnt++;

                if (selectedKeys != 0 || oldWakenUp || wakenUp.get() || opWriteUpdater.wasChanged()) {
                    // - Selected something,
                    // - waken up by user, or
                    // - the task queue has a pending task.
                    // - a scheduled task is ready for processing
                    break;
                }
                if (Thread.interrupted()) {
                    // Thread was interrupted so reset selected keys and break so we not run into a busy loop.
                    // As this is most likely a bug in the handler of the user or it's client library we will
                    // also log it.
                    //
                    // See https://github.com/netty/netty/issues/2426
                    if (logger.isDebugEnabled()) {
                        logger.debug("Selector.select() returned prematurely because " +
                                "Thread.currentThread().interrupt() was called. Use " +
                                "NioEventLoop.shutdownGracefully() to shutdown the NioEventLoop.");
                    }
                    selectCnt = 1;
                    break;
                }

                long time = System.nanoTime();
                if (time - TimeUnit.MILLISECONDS.toNanos(timeoutMillis) >= currentTimeNanos) {
                    // timeoutMillis elapsed without anything selected.
                    selectCnt = 1;
                } else if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                        selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
                    // The selector returned prematurely many times in a row.
                    // Rebuild the selector to work around the problem.
                    logger.warn(
                            "Selector.select() returned prematurely {} times in a row; rebuilding selector.",
                            selectCnt);

                    rebuildSelector();
                    selector = this.selector;

                    // Select again to populate selectedKeys.
                    selector.selectNow();
                    selectCnt = 1;
                    break;
                }

                currentTimeNanos = time;
            }

            if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Selector.select() returned prematurely {} times in a row.", selectCnt - 1);
                }
            }
        } catch (CancelledKeyException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector - JDK bug?", e);
            }
            // Harmless exception - log anyway
        }
    }

    /**
     * Replaces the current {@link Selector} of this event loop with newly created {@link Selector}s
     * to work around the infamous epoll 100% CPU bug.
     */
    void rebuildSelector() {


        final Selector oldSelector = selector;
        final Selector newSelector;

        if (oldSelector == null) {
            return;
        }

        try {
            newSelector = openSelector();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return;
        }

        // Register all channels to the new Selector.
        int nChannels = 0;
        for (; ; ) {
            try {
                for (SelectionKey key : oldSelector.keys()) {
                    Object a = key.attachment();
                    try {
                        if (!key.isValid() || key.channel().keyFor(newSelector) != null) {
                            continue;
                        }

                        int interestOps = key.interestOps();
                        key.cancel();
                        SelectionKey newKey = key.channel().register(newSelector, interestOps, a);
                        if (a instanceof AbstractNioChannel) {
                            // Update SelectionKey
                            ((AbstractNioChannel) a).selectionKey = newKey;
                        }
                        nChannels++;
                    } catch (Exception e) {
                        logger.warn("Failed to re-register a Channel to the new Selector.", e);
                        if (a instanceof AbstractNioChannel) {
                            AbstractNioChannel ch = (AbstractNioChannel) a;
                            ch.unsafe().close(ch.unsafe().voidPromise());
                        } else {
                            @SuppressWarnings("unchecked")
                            NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                            invokeChannelUnregistered(task, key, e);
                        }
                    }
                }
            } catch (ConcurrentModificationException e) {
                // Probably due to concurrent modification of the key set.
                continue;
            }

            break;
        }

        selector = newSelector;

        try {
            // time to close the old selector as everything else is registered to the new one
            oldSelector.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the old Selector.", t);
            }
        }

        logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
    }


    private void selectAgain() {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }


}


