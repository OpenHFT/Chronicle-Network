package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.performance.tests.network.EchoHandler;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class EchoServer2Main {
    public static <T extends VanillaNetworkContext<T>> void main(String[] args) throws IOException {
        System.setProperty("pauser.minProcessors", "1");
        Affinity.acquireCore();
        @NotNull EventLoop eg = EventGroup.builder().withDaemon(false).withPauser(Pauser.busy()).withBinding("any").build();
        eg.start();

        @NotNull AcceptorEventHandler<T> eah = new AcceptorEventHandler<T>("*:" + EchoClientMain.PORT,
                nc -> {
                    TcpEventHandler<T> teh = new TcpEventHandler<T>(nc);
                    teh.tcpHandler(new EchoHandler<>());
                    return teh;
                },
                () -> (T) new VanillaNetworkContext());
        eg.addHandler(eah);
    }
}
