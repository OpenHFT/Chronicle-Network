/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.performance.tests.network2;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network2.AcceptorEventHandler;
import net.openhft.chronicle.network2.TcpHandler;
import net.openhft.chronicle.network2.event.EventGroup;

import net.openhft.performance.tests.vanilla.tcp.EchoClientMain;

import java.io.IOException;

/**
 * Created by peter.lawrey on 22/01/15.
 */
class EchoHandler implements TcpHandler {
    @Override
    public void process(Bytes in, Bytes out) {
        if (in.remaining() == 0)
            return;
//            System.out.println("P - " + in.readLong(in.position()) + " " + in.toDebugString());
        long toWrite = Math.min(in.remaining(), out.remaining());
        out.write(in, in.position(), toWrite);
        out.skip(toWrite);
    }

    public static void main(String[] args) throws IOException {
        EventGroup eg = new EventGroup();
        eg.start();
        AcceptorEventHandler eah = new AcceptorEventHandler(EchoClientMain.PORT, EchoHandler::new);
        eg.addHandler(eah);
    }
}
