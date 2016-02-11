package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.ConnectionDetails;
import net.openhft.chronicle.network.ConnectorEventHandler;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
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


        Map<String, ConnectionDetails> nameToConnectionDetails = new ConcurrentHashMap<>();
        ConnectionDetails test1 = new ConnectionDetails("Test1", "localhost:5678");
        nameToConnectionDetails.put("Test1", test1);

        EventLoop eg2 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg2.start();
        ConnectorEventHandler ceh = new ConnectorEventHandler(nameToConnectionDetails,
                cd -> new TcpClientHandler(cd, messages), VanillaSessionDetails::new, 0,0);
        ceh.unchecked(true);
        eg2.addHandler(ceh);

        EventLoop eg1 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg1.start();
        AcceptorEventHandler eah = new AcceptorEventHandler("localhost:5678",
                TcpServerHandler::new, VanillaSessionDetails::new, 0,0);
        eah.unchecked(true);
        eg1.addHandler(eah);
        //Give the server a chance to start
        Jvm.pause(200);
        ceh.forceRescan();

        Jvm.pause(2000);
        //should receive 3 messages
        Assert.assertEquals(2, messages.size());
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

    private class TcpServerHandler implements TcpHandler{
        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, @NotNull SessionDetailsProvider sessionDetails) {
            if (in.readRemaining() == 0)
                return;
            int v = in.readInt();
            System.out.println("Server receives:" + v);
            System.out.println("Server sends:" + Math.sqrt(v));
            out.writeDouble(Math.sqrt(v));
        }
    }

    private class TcpClientHandler implements TcpHandler{
        AtomicInteger i = new AtomicInteger(2);
        private ConnectionDetails cd;
        private List<String> messages;

        public TcpClientHandler(ConnectionDetails cd, List<String> messages) {
            this.messages = messages;
            i.set(2);
            this.cd = cd;

            Executors.newSingleThreadExecutor().submit(()->{
                while (true) {
                    Jvm.pause(1000);
                    i.set(2);
                }
            });
        }

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, @NotNull SessionDetailsProvider sessionDetails) {
            if (in.readRemaining() != 0) {
                double v = in.readDouble();
                messages.add("Client " + cd.getName() + " receives:" + v);
                System.out.println("Client " + cd.getName() + " receives:" + v);
            }
            if(i.get() != -1) {
                System.out.println("Client sends:" + i.get());
                out.writeInt(i.get());
                i.set(-1);
            }
        }
    }
}
