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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by daniel on 09/02/2016.
 */
public class ConnectionEventHandlerTest {
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
    @Ignore("TODO FIX Doesn't appear to be using Text or Binary wire")
    // This test verifies that we can create a ConnectionEventHandler and that we can disable and
    // re-enable the client at will. Tests that a client can be connected before the server is ready.
    //
    // 1. It then start up a client (Test we can setup a connection before the server is ready)
    // 2. This tests fires up a TCP server
    // 3. The client sends a request to the server every second and the server sends a response to the client
    //    which is recorded
    // 4. After 2 seconds we test that the client has received 2 responses from the client
    // 5. The client is then disabled
    // 6. After 2 seconds we tests that no messages have been received from the client
    // 7. The client is then enabled
    // 8. After 2 seconds we test that the client has received 2 responses from the client
    public void testConnection() throws IOException {
        @NotNull List<String> messages = new ArrayList<>();

        TCPRegistry.createServerSocketChannelFor("host.port1");

        @NotNull Map<String, ConnectionDetails> nameToConnectionDetails = new ConcurrentHashMap<>();
        @NotNull ConnectionDetails test1 = new ConnectionDetails("Test1", "host.port1");
        nameToConnectionDetails.put("Test1", test1);

        @NotNull EventLoop eg2 = new EventGroup(false, BusyPauser.INSTANCE, true);
        eg2.start();

        final ConnectionStrategy connectionStrategy = new AlwaysStartOnPrimaryConnectionStrategy();
        final SocketConnectionNotifier socketConnectionNotifier = SocketConnectionNotifier.newDefaultConnectionNotifier();

        @NotNull ConnectorEventHandler ceh = new ConnectorEventHandler(nameToConnectionDetails,
                cd -> new TcpClientHandler(cd, messages),
                VanillaSessionDetails::new,
                connectionStrategy,
                socketConnectionNotifier);

        eg2.addHandler(ceh);

        @NotNull EventLoop eg1 = new EventGroup(false, BusyPauser.INSTANCE, true);
        eg1.start();
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler("host.port1",
                LegacyHanderFactory.defaultTcpEventHandlerFactory(TcpServerHandler::new),
                VanillaNetworkContext::new);

        //  eah.unchecked(true);
        eg1.addHandler(eah);
        //Give the server a chance to start
        Jvm.pause(200);
        ceh.forceRescan();

        Jvm.pause(500);
        //should receive 2 or 3 depends on timing messages
        Assert.assertTrue(messages.size() == 2 || messages.size() == 3);
        messages.clear();

        System.out.println("** DISABLE");
        test1.setDisable(true);
        ceh.forceRescan();

        //should receive no messages

        Jvm.pause(500);
        Assert.assertEquals(0, messages.size());
        messages.clear();

        System.out.println("** ENABLE");
        test1.setDisable(false);
        ceh.forceRescan();

        //should receive 3 messages
        Jvm.pause(500);
        Assert.assertEquals(2, messages.size());
    }

    private class TcpServerHandler implements TcpHandler {
        public <T extends NetworkContext> TcpServerHandler(T t) {

        }

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out) {
            if (in.readRemaining() == 0)
                return;
            int v = in.readInt();
            System.out.println("Server receives:" + v);
            System.out.println("Server sends:" + Math.sqrt(v));
            out.writeDouble(Math.sqrt(v));
        }
    }

    private class TcpClientHandler implements TcpHandler {
        @NotNull
        AtomicInteger i = new AtomicInteger(2);
        private ConnectionDetails cd;
        private List<String> messages;

        public TcpClientHandler(ConnectionDetails cd, List<String> messages) {
            this.messages = messages;
            i.set(2);
            this.cd = cd;

            Executors.newSingleThreadExecutor().submit(() -> {
                while (true) {
                    Jvm.pause(500);
                    i.set(2);
                }
            });
        }

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out) {
            if (in.readRemaining() != 0) {
                double v = in.readDouble();
                messages.add("Client " + cd.getID() + " receives:" + v);
                System.out.println("Client " + cd.getID() + " receives:" + v);
            }
            if (i.get() != -1) {
                System.out.println("Client sends:" + i.get());
                out.writeInt(i.get());
                i.set(-1);
            }
        }
    }
}
