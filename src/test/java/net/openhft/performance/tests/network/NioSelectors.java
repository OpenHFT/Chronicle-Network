package net.openhft.performance.tests.network;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.BufferUnderflowException;
import java.nio.channels.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.channels.SelectionKey.OP_WRITE;

public class NioSelectors {

    private static final long SPIN_LOOP_TIME_IN_NONOSECONDS = 10000;
    private static final int BUFFER_SIZE = 1024;
    Logger LOG = LoggerFactory.getLogger(NioSelectors.class);
    private final Queue<Runnable> pendingRegistrations = new ConcurrentLinkedQueue<Runnable>();
    private volatile boolean isClosed = false;
    final SelectedSelectionKeySet selectedKeys = new SelectedSelectionKeySet();

    final String name = "";

    private final SelectionKey[] selectionKeysStore = new SelectionKey[Byte.MAX_VALUE + 1];
    List<Closeable> closeables = new ArrayList<>();
    final Selector selector = openSelector(closeables);

    private final KeyInterestUpdater opWriteUpdater =
            new KeyInterestUpdater(OP_WRITE, selectionKeysStore);
    private boolean useJavaNIOSelectionKeys;
    private long selectorTimeout;

    Details serverDetails = null; // todo
    Details clientDetails = null; // todo

    public NioSelectors() throws IOException {
    }


    void processEvent() throws IOException {
        try {

            int serverPort = 8080;
            boolean useJavaNIOSelectionKeys = false;

            final InetSocketAddress serverInetSocketAddress =
                    new InetSocketAddress(serverPort);

            //  final Details serverDetails = new Details(serverInetSocketAddress, localIdentifier);
            new ServerConnector(serverDetails).doConnect();

           /* for (InetSocketAddress client : replicationConfig.endpoints()) {
                //   final Details clientDetails = new Details(client, localIdentifier);
                new ClientConnector(clientDetails).doConnect();
            }*/

            while (selector.isOpen()) {
                registerPendingRegistrations();

                final int nSelectedKeys = select();

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();

                checkThrottleInterval();

                // check that we have sent and received heartbeats
                heartBeatMonitor(approxTime);

                // set the OP_WRITE when data is ready to send
                opWriteUpdater.applyUpdates();

                if (useJavaNIOSelectionKeys) {
                    // use the standard java nio selector

                    if (nSelectedKeys == 0)
                        continue;    // go back and check pendingRegistrations

                    final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    for (final SelectionKey key : selectionKeys) {
                        processKey(approxTime, key);
                    }
                    selectionKeys.clear();
                } else {
                    // use the netty like selector

                    final SelectionKey[] keys = selectedKeys.flip();

                    try {
                        for (int i = 0; i < keys.length && keys[i] != null; i++) {
                            final SelectionKey key = keys[i];

                            try {
                                processKey(approxTime, key);
                            } catch (BufferUnderflowException e) {
                                if (!isClosed)
                                    LOG.error("", e);
                            }
                        }
                    } finally {
                        for (int i = 0; i < keys.length && keys[i] != null; i++) {
                            keys[i] = null;
                        }
                    }
                }
            }
        } catch (CancelledKeyException | ConnectException | ClosedChannelException |
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

    Selector openSelector(final List<Closeable> closeables) throws IOException {

        Selector result = Selector.open();
        this.closeables.add(result);

        if (!useJavaNIOSelectionKeys) {
            this.closeables.add((net.openhft.chronicle.core.io.Closeable) () -> {
                        {
                            SelectionKey[] keys = selectedKeys.flip();
                            for (int i = 0; i < keys.length && keys[i] != null; i++) {
                                keys[i] = null;
                            }
                        }
                        {
                            SelectionKey[] keys = selectedKeys.flip();
                            for (int i = 0; i < keys.length && keys[i] != null; i++) {
                                keys[i] = null;
                            }
                        }
                    }
            );
            return openSelector(result, selectedKeys);
        }

        return result;
    }

    /**
     * this is similar to the code in Netty
     *
     * @param selector
     * @return
     */
    private Selector openSelector(@NotNull final Selector selector,
                                  @NotNull final SelectedSelectionKeySet selectedKeySet) {
        try {

            Class<?> selectorImplClass =
                    Class.forName("sun.nio.ch.SelectorImpl", false, getSystemClassLoader());

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

            //   logger.trace("Instrumented an optimized java.util.Set into: {}", selector);
        } catch (Exception e) {
            LOG.error("", e);
            //   logger.trace("Failed to instrument an optimized java.util.Set into: {}", selector, t);
        }

        return selector;
    }

    private void heartBeatMonitor(final long approxTime) {
    }

    private void checkThrottleInterval() {

    }

    private void closeResources() {

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
            /// } catch (InterruptedException e) {
            //   quietClose(key, e);
        } catch (BufferUnderflowException |
                ClosedSelectorException | CancelledKeyException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (Exception e) {
            LOG.info("", e);
            if (!isClosed)
                closeEarlyAndQuietly(key.channel());
        }
    }

    private void closeEarlyAndQuietly(final SelectableChannel channel) {
    }

    private void quietClose(final SelectionKey key, final Exception e) {

    }

    private void onWrite(final SelectionKey key, final long approxTime) {
    }

    private void onRead(final SelectionKey key, final long approxTime) {

    }

    private void onConnect(final SelectionKey selectionKey) {

    }

    private void onAccept(final SelectionKey key) {

    }

    /**
     * sets interestOps to "selector keys",The change to interestOps much be on the same thread as the selector. This class, allows via {@link
     * AbstractChannelReplicator .KeyInterestUpdater#set(int)}  to holds a pending change  in interestOps ( via a bitset ), this change is processed
     * later on the same thread as the selector
     */
    private class KeyInterestUpdater {

        private final AtomicBoolean wasChanged = new AtomicBoolean();
        @NotNull
        private final BitSet changeOfOpWriteRequired;
        @NotNull
        private final SelectionKey[] selectionKeys;
        private final int op;

        KeyInterestUpdater(int op, @NotNull final SelectionKey[] selectionKeys) {
            this.op = op;
            this.selectionKeys = selectionKeys;
            changeOfOpWriteRequired = new BitSet(selectionKeys.length);
        }

        public void applyUpdates() {
            if (wasChanged.getAndSet(false)) {
                for (int i = changeOfOpWriteRequired.nextSetBit(0); i >= 0;
                     i = changeOfOpWriteRequired.nextSetBit(i + 1)) {
                    changeOfOpWriteRequired.clear(i);
                    final SelectionKey key = selectionKeys[i];
                    try {
                        key.interestOps(key.interestOps() | op);
                    } catch (Exception e) {
                        LOG.debug("", e);
                    }
                }
            }
        }

        /**
         * @param keyIndex the index of the key that has changed, the list of keys is provided by the constructor {@link KeyInterestUpdater (int,
         *                 SelectionKey[])}
         */
        public void set(int keyIndex) {
            changeOfOpWriteRequired.set(keyIndex);
            wasChanged.lazySet(true);
        }
    }

    /**
     * spin loops 100000 times first before calling the selector with timeout
     *
     * @return The number of keys, possibly zero, whose ready-operation sets were updated
     * @throws IOException
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

    static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();
        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return ClassLoader.getSystemClassLoader();
                }
            });
        }
    }

    void registerPendingRegistrations() throws ClosedChannelException {
        for (Runnable runnable = pendingRegistrations.poll(); runnable != null;
             runnable = pendingRegistrations.poll()) {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.info("", e);
            }
        }
    }

    private class ServerConnector {

        @NotNull
        private final Details details;

        private ServerConnector(@NotNull Details details) {
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
        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = openServerSocketChannel();

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, 0);
            ServerSocket serverSocket = null;

            try {
                serverSocket = serverChannel.socket();
            } finally {
                if (serverSocket != null)
                    closeables.add(serverSocket);
            }

            serverSocket.setReuseAddress(true);

            serverSocket.bind(details.address());

            // these can be run on this thread
         /*   addPendingRegistration(new Runnable() {
                @Override
                public void run() {
                    final TcpReplicator.Attached attached = new TcpReplicator.Attached();
                    attached.connector = TcpReplicator.ServerConnector.this;
                    try {
                        serverChannel.register(TcpReplicator.this.selector, OP_ACCEPT, attached);
                    } catch (ClosedChannelException e) {
                        LOG.debug("", e);
                    }
                }
            });*/

            selector.wakeup();

            return serverChannel;
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

    private class ClientConnector {

        @NotNull
        private final Details details;

        private ClientConnector(@NotNull Details details) {

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
        SelectableChannel doConnect() throws IOException, InterruptedException {
            boolean success = false;

            final SocketChannel socketChannel = openSocketChannel(closeables);

            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setReuseAddress(true);
                socketChannel.socket().setSoLinger(false, 0);
                socketChannel.socket().setSoTimeout(0);

                try {
                    socketChannel.connect(details.address());
                } catch (UnresolvedAddressException e) {
                    //   this.connectLater();
                }

                // Under experiment, the concoction was found to be more successful if we
                // paused before registering the OP_CONNECT
                Thread.sleep(10);

                // the registration has be be run on the same thread as the selector
             /*   addPendingRegistration(new Runnable() {
                    @Override
                    public void run() {
                        final TcpReplicator.Attached attached = new TcpReplicator.Attached();
                        attached.connector =  ClientConnector.this;

                        try {
                            socketChannel.register(selector, OP_CONNECT, attached);
                        } catch (ClosedChannelException e) {
                            if (socketChannel.isOpen())
                                LOG.error("", e);
                        }
                    }
                });*/

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

    static SocketChannel openSocketChannel(final List<Closeable> closeables) throws IOException {
        SocketChannel result = null;

        try {
            result = SocketChannel.open();
            result.socket().setTcpNoDelay(true);
        } finally {
            if (result != null)
                try {
                    closeables.add(result);
                } catch (IllegalStateException e) {
                    // already closed
                }
        }
        return result;
    }

    /**
     * details about the socket connection
     */
    static class Details {

        private final InetSocketAddress address;
        private final byte localIdentifier;

        Details(@NotNull final InetSocketAddress address, final byte localIdentifier) {
            this.address = address;
            this.localIdentifier = localIdentifier;
        }

        public InetSocketAddress address() {
            return address;
        }

        public byte localIdentifier() {
            return localIdentifier;
        }

        @Override
        public String toString() {
            return "Details{" +
                    "address=" + address +
                    ", localIdentifier=" + localIdentifier +
                    '}';
        }
    }

}
