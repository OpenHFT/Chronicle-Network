/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.LegacyHandedFactory;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.performance.tests.vanilla.tcp.EchoClientMain;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Created by peter.lawrey on 22/01/15.
 */
class TimedEchoHandler implements TcpHandler {

    public <T extends NetworkContext> TimedEchoHandler(T t) {

    }

    public static void main(String[] args) throws IOException {
        EventLoop eg = new EventGroup(false);
        eg.start();
        AcceptorEventHandler eah = new AcceptorEventHandler("*:" + EchoClientMain.PORT,
                LegacyHandedFactory.legacyTcpEventHandlerFactory(TimedEchoHandler::new),
                VanillaNetworkContext::new);


        eg.addHandler(eah);
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out) {
        if (in.readRemaining() == 0)
            return;
//        System.out.println("P start " + in.toDebugString());
        long toWrite = Math.min(in.readRemaining(), out.writeRemaining());
        out.write(in, in.readPosition(), toWrite);
        out.writeLong(System.nanoTime());
        in.readSkip(toWrite);
//        System.out.println("... P End " + in.toDebugString());
    }
}
