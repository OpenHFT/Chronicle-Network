/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
