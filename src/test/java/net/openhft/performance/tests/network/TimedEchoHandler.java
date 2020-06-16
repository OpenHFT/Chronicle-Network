/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
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
package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.performance.tests.vanilla.tcp.EchoClientMain;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

class TimedEchoHandler<T extends NetworkContext<T>>
        extends SimpleCloseable
        implements TcpHandler<T> {

    public TimedEchoHandler(T t) {

    }

    public static <T extends NetworkContext<T>> void main(String[] args) throws IOException {
        @NotNull EventLoop eg = new EventGroup(false);
        eg.start();
        @NotNull AcceptorEventHandler<T> eah = new AcceptorEventHandler<>("*:" + EchoClientMain.PORT,
                LegacyHanderFactory.legacyTcpEventHandlerFactory(TimedEchoHandler::new),
                () -> (T) new VanillaNetworkContext<>());

        eg.addHandler(eah);
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, T nc) {
        if (in.readRemaining() == 0)
            return;
        long toWrite = Math.min(in.readRemaining(), out.writeRemaining());
        out.write(in, in.readPosition(), toWrite);
        out.writeLong(System.nanoTime());
        in.readSkip(toWrite);
    }
}
