/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

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
    @Test
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
        List<String> messages = new ArrayList<>();

        TCPRegistry.createServerSocketChannelFor("host.port1");

        Map<String, ConnectionDetails> nameToConnectionDetails = new ConcurrentHashMap<>();
        ConnectionDetails test1 = new ConnectionDetails("Test1", "host.port1");
        nameToConnectionDetails.put("Test1", test1);

        EventLoop eg2 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg2.start();
        ConnectorEventHandler ceh = new ConnectorEventHandler(nameToConnectionDetails,
                cd -> new TcpClientHandler(cd, messages), VanillaSessionDetails::new);
        //     ceh.unchecked(true);
        eg2.addHandler(ceh);

        EventLoop eg1 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg1.start();
        AcceptorEventHandler eah = new AcceptorEventHandler("host.port1",
                LegacyHanderFactory.simpleTcpEventHandlerFactory(TcpServerHandler::new, WireType.TEXT),
                VanillaNetworkContext::new);

        //  eah.unchecked(true);
        eg1.addHandler(eah);
        //Give the server a chance to start
        Jvm.pause(200);
        ceh.forceRescan();

        Jvm.pause(2000);
        //should receive 2 or 3 depends on timing messages
        Assert.assertTrue(messages.size() == 2 || messages.size() == 3);
        messages.clear();

        System.out.println("** DISABLE");
        test1.setDisable(true);
        ceh.forceRescan();

        //should receive no messages

        Jvm.pause(2000);
        Assert.assertEquals(0, messages.size());
        messages.clear();

        System.out.println("** ENABLE");
        test1.setDisable(false);
        ceh.forceRescan();

        //should receive 3 messages
        Jvm.pause(2000);
        Assert.assertEquals(2, messages.size());
    }

    private class TcpServerHandler implements TcpHandler {
        public <T extends NetworkContext> TcpServerHandler(T t) {

        }

        @Override
        public void process(@org.jetbrains.annotations.NotNull @NotNull Bytes in, @org.jetbrains.annotations.NotNull @NotNull Bytes out) {
            if (in.readRemaining() == 0)
                return;
            int v = in.readInt();
            System.out.println("Server receives:" + v);
            System.out.println("Server sends:" + Math.sqrt(v));
            out.writeDouble(Math.sqrt(v));
        }
    }

    private class TcpClientHandler implements TcpHandler {
        AtomicInteger i = new AtomicInteger(2);
        private ConnectionDetails cd;
        private List<String> messages;

        public TcpClientHandler(ConnectionDetails cd, List<String> messages) {
            this.messages = messages;
            i.set(2);
            this.cd = cd;

            Executors.newSingleThreadExecutor().submit(() -> {
                while (true) {
                    Jvm.pause(1000);
                    i.set(2);
                }
            });
        }

        @Override
        public void process(@org.jetbrains.annotations.NotNull @NotNull Bytes in, @org.jetbrains.annotations.NotNull @NotNull Bytes out) {
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
