package net.openhft.performance.tests.network2;

import net.openhft.chronicle.network2.AcceptorEventHandler;
import net.openhft.chronicle.network2.TcpHandler;
import net.openhft.chronicle.network2.event.EventGroup;
import net.openhft.performance.tests.vanilla.tcp.EchoClientMain;
import net.openhft.lang.io.Bytes;

import java.io.IOException;

/**
 * Created by peter on 22/01/15.
 */
class EchoHandler implements TcpHandler {
    @Override
    public void process(Bytes in, Bytes out) {
        if (in.remaining() == 0)
            return;
//            System.out.println("P - " + in.readLong(in.position()) + " " + in.toDebugString());
        out.write(in);
    }

    public static void main(String[] args) throws IOException {
        EventGroup eg = new EventGroup();
        eg.start();
        AcceptorEventHandler eah = new AcceptorEventHandler(EchoClientMain.PORT, EchoHandler::new);
        eg.addHandler(eah);
    }
}
