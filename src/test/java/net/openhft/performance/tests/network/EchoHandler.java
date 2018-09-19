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
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.TcpHandler;
import org.jetbrains.annotations.NotNull;

/*
 * Created by peter.lawrey on 22/01/15.
 */
public class EchoHandler implements TcpHandler<NetworkContext> {

    public EchoHandler() {
    }

    @Override
    public void process(@NotNull final Bytes in, @NotNull final Bytes out, NetworkContext nc) {
//        System.out.println(in.readRemaining());
        if (in.readRemaining() == 0)
            return;
//        System.out.println("P start " + in.toDebugString());
        long toWrite = Math.min(in.readRemaining(), out.writeRemaining());
        out.write(in, in.readPosition(), toWrite);
        in.readSkip(toWrite);
//        System.out.println("... P End " + in.toDebugString());
    }
}
