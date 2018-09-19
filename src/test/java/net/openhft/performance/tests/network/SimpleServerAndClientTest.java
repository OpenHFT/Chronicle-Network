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

package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.connection.FatalFailureConnectionStrategy;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.network.connection.TryLock;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.network.connection.SocketAddressSupplier.uri;

/*
 * Created by rob on 26/08/2015.
 */
public class SimpleServerAndClientTest {
    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {

        threadDump.assertNoNewThreads();
    }

    @Test
    public void test() throws IOException {
        YamlLogging.setAll(false);

        for (; ; ) {
            // this the name of a reference to the host name and port,
            // allocated automatically when to a free port on localhost
            @NotNull final String desc = "host.port";
            TCPRegistry.createServerSocketChannelFor(desc);

            // we use an event loop rather than lots of threads
            try (@NotNull EventLoop eg = new EventGroup(true)) {
                eg.start();

                // an example message that we are going to send from the server to the client and back
                @NotNull final String expectedMessage = "<my message>";
                createServer(desc, eg);

                try (@NotNull TcpChannelHub tcpChannelHub = createClient(eg, desc)) {

                    // create the message the client sends to the server

                    // the tid must be unique, its reflected back by the server, it must be at the start
                    // of each message sent from the server to the client. Its use by the client to identify which
                    // thread will handle this message
                    final long tid = tcpChannelHub.nextUniqueTransaction(System.currentTimeMillis());

                    // we will use a text wire backed by a elasticByteBuffer
                    @NotNull final Wire wire = new TextWire(Bytes.elasticByteBuffer()).useTextDocuments();

                    wire.writeDocument(true, w -> w.write(() -> "tid").int64(tid));
                    wire.writeDocument(false, w -> w.write(() -> "payload").text(expectedMessage));

                    // write the data to the socket
                    tcpChannelHub.lock2(() -> tcpChannelHub.writeSocket(wire, true),
                            true, TryLock.TRY_LOCK_WARN);

                    // read the reply from the socket ( timeout after 5 second ), note: we have to pass
                    // the tid
                    try {
                        Wire reply = tcpChannelHub.proxyReply(5000, tid);

                        // read the reply and check the result
                        reply.readDocument(null, data -> {
                            @Nullable final String text = data.read(() -> "payloadResponse").text();
                            Assert.assertEquals(expectedMessage, text);
                        });

                    } catch (TimeoutException e) {
                        // retry, you will get this is the client attempts to send a message to
                        // the server and the server is not running or ready
                        // note :  that net.openhft.chronicle.network.TCPRegistry
                        // .createServerSocketChannelFor()
                        // opens a server socket, that does not mean that there is a server
                        // running to read this, for example if you commented out "createServer(desc, eg); "
                        // and this time out exception will be thrown
                        continue;
                    }
                    break;
                }

            } finally {
                TcpChannelHub.closeAllHubs();
                TCPRegistry.reset();
            }
        }

    }

    @NotNull
    private TcpChannelHub createClient(@NotNull EventLoop eg, String desc) {
        return new TcpChannelHub(null,
                eg, WireType.TEXT, "/", uri(desc), false,
                null, HandlerPriority.TIMER, new FatalFailureConnectionStrategy(3));
    }

    private void createServer(@NotNull String desc, @NotNull EventLoop eg) throws IOException {
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                LegacyHanderFactory.simpleTcpEventHandlerFactory(WireEchoRequestHandler::new, WireType.TEXT),
                VanillaNetworkContext::new);

        eg.addHandler(eah);
        ServerSocketChannel sc = TCPRegistry.acquireServerSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
