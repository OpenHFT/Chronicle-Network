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
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.network.connection.TryLock;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.network.connection.SocketAddressSupplier.uri;

/**
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
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();

        threadDump.assertNoNewThreads();
    }

    @Test
    // @Ignore("Fails on Teamcity ")
    public void test() throws IOException, TimeoutException {
        // this the name of a reference to the host name and port,
        // allocated automatically when to a free port on localhost
        final String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);

        // we use an event loop rather than lots of threads
        EventLoop eg = new EventGroup(true);
        eg.start();


        // an example message that we are going to send from the server to the client and back
        final String expectedMessage = "<my message>";
        createServer(desc, eg);

        try (TcpChannelHub tcpChannelHub = createClient(eg, desc)) {

            // create the message the client sends to the server

            // the tid must be unique, its reflected back by the server, it must be at the start
            // of each message sent from the server to the client. Its use by the client to identify which
            // thread will handle this message
            final long tid = tcpChannelHub.nextUniqueTransaction(System.currentTimeMillis());

            // we will use a text wire backed by a elasticByteBuffer
            final Wire wire = new TextWire(Bytes.elasticByteBuffer());

            wire.writeDocument(true, w -> w.write(() -> "tid").int64(tid));
            wire.writeDocument(false, w -> w.write(() -> "payload").text(expectedMessage));

            // write the data to the socket
            tcpChannelHub.lock2(() -> tcpChannelHub.writeSocket(wire, true),
                    true, TryLock.TRY_LOCK_WARN);

            // read the reply from the socket ( timeout after 1 second ), note: we have to pass the tid
            Wire reply = tcpChannelHub.proxyReply(TimeUnit.SECONDS.toMillis(5), tid);

            // read the reply and check the result
            reply.readDocument(null, data -> {
                final String text = data.read(() -> "payloadResponse").text();
                Assert.assertEquals(expectedMessage, text);
            });

        }
        eg.close();
    }

    @NotNull
    private TcpChannelHub createClient(EventLoop eg, String desc) {
        return new TcpChannelHub(null, eg, WireType.TEXT, "/", uri(desc), false, null, HandlerPriority.TIMER);
    }

    private void createServer(String desc, EventLoop eg) throws IOException {
        AcceptorEventHandler eah = new AcceptorEventHandler(desc,

                LegacyHanderFactory.simpleTcpEventHandlerFactory(WireEchoRequestHandler::new, WireType.TEXT),
                VanillaNetworkContext::new);

        eg.addHandler(eah);
        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
