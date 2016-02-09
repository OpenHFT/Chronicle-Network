package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.ConnectorEventHandler;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by daniel on 09/02/2016.
 */
public class ConnectionEventHandlerTest {
    @Test
    public void testConnection() throws IOException {
        Map nameToConnectionDetails = new HashMap<>();
        nameToConnectionDetails.put("Test1", new ConnectorEventHandler.ConnectionDetails("Test1", "localhost:5678"));

        EventGroup eg2 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg2.start();
        ConnectorEventHandler ceh = new ConnectorEventHandler(nameToConnectionDetails,
                ()-> new MyTcpClientHandler(), VanillaSessionDetails::new, 0,0);
        ceh.unchecked(true);
        eg2.addHandler(ceh);

        EventGroup eg1 = new EventGroup(false, Throwable::printStackTrace, BusyPauser.INSTANCE, true);
        eg1.start();
        AcceptorEventHandler eah = new AcceptorEventHandler("localhost:5678",
                ()-> new MyTcpServerHandler(), VanillaSessionDetails::new, 0,0);
        eah.unchecked(true);
        eg1.addHandler(eah);

        Jvm.pause(500);
    }

    private class MyTcpServerHandler implements TcpHandler{
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

    private class MyTcpClientHandler implements TcpHandler{
        AtomicInteger i = new AtomicInteger(2);
        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, @NotNull SessionDetailsProvider sessionDetails) {
            if (in.readRemaining() != 0) {
                System.out.println("Client receives:" + in.readDouble());
            }
            if(i.get() != -1) {
                System.out.println("Client sends:" + i.get());
                out.writeInt(i.get());
                i.set(-1);
            }
        }
    }
}
