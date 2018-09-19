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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static net.openhft.performance.tests.network.LegacyHanderFactory.simpleTcpEventHandlerFactory;

/*
Running on an i7-3970X

TextWire: Loop back echo latency was 7.4/8.9 12/20 108/925 us for 50/90 99/99.9 99.99/worst %tile
BinaryWire: Loop back echo latency was 6.6/8.0 9/11 19/3056 us for 50/90 99/99.9 99.99/worst %tile
RawWire: Loop back echo latency was 5.9/6.8 8/10 12/80 us for 50/90 99/99.9 99.99/worst %tile
 */
@RunWith(value = Parameterized.class)
public class WireTcpHandlerTest {

    public static final int SIZE_OF_SIZE = 4;

    private final String desc;
    private final WireType wireType;

    public WireTcpHandlerTest(String desc, WireType wireWrapper) {
        this.desc = desc;
        this.wireType = wireWrapper;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> combinations() {
        return Arrays.asList(
                new Object[]{"TextWire", WireType.TEXT},
                new Object[]{"BinaryWire", WireType.BINARY}
                //,
                //   new Object[]{"RawWire", WireType.RAW}
        );
    }

    private static void testLatency(String desc, @NotNull Function<Bytes, Wire> wireWrapper, @NotNull SocketChannel... sockets) throws IOException {
        int tests = 40000;
        @NotNull long[] times = new long[tests * sockets.length];
        int count = 0;
        ByteBuffer out = ByteBuffer.allocateDirect(64 * 1024);
        Bytes outBytes = Bytes.wrapForWrite(out);
        Wire outWire = wireWrapper.apply(outBytes);

        ByteBuffer in = ByteBuffer.allocateDirect(64 * 1024);
        Bytes inBytes = Bytes.wrapForRead(in);
        Wire inWire = wireWrapper.apply(inBytes);
        @NotNull TestData td = new TestData();
        @NotNull TestData td2 = new TestData();
        for (int i = -12000; i < tests; i++) {
            long now = System.nanoTime();
            for (@NotNull SocketChannel socket : sockets) {
                out.clear();
                outBytes.clear();
                td.value3 = td.value2 = td.value1 = i;
                try (DocumentContext ignored = outWire.writingDocument(false)) {
                    td.write(outWire);
                }
                out.limit((int) outBytes.writePosition());
                socket.write(out);
                if (out.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }

            for (@NotNull SocketChannel socket : sockets) {
                in.clear();
                inBytes.clear();
                while (true) {
                    int read = socket.read(in);
                    inBytes.readLimit(in.position());
                    if (inBytes.readRemaining() >= SIZE_OF_SIZE) {
                        int header = inBytes.readInt(0);

                        final int len = Wires.lengthOf(header);
                        if (inBytes.readRemaining() >= len) {
                            td2.read(inWire);
                        }
                        break;
                    }
                    if (read < 0)
                        throw new AssertionError("Unable to read in one go.");
                }
                if (i >= 0)
                    times[count++] = System.nanoTime() - now;
            }
        }
        Arrays.sort(times);
        System.out.printf("%s: Loop back echo latency was %.1f/%.1f %,d/%,d %,d/%d us for 50/90 99/99.9 99.99/worst %%tile%n",
                desc,
                times[times.length / 2] / 1e3,
                times[times.length * 9 / 10] / 1e3,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - 1] / 1000
        );
    }

    @Test
    public void testProcess() throws IOException {
        @NotNull EventLoop eg = new EventGroup(true);
        eg.start();
        TCPRegistry.createServerSocketChannelFor(desc);
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                simpleTcpEventHandlerFactory(EchoRequestHandler::new, wireType),
                VanillaNetworkContext::new);
        eg.addHandler(eah);

        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);

        //       testThroughput(sc);
        testLatency(desc, wireType, sc);

        eg.stop();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    static class EchoRequestHandler extends WireTcpHandler {

        private final TestData td = new TestData();

        public EchoRequestHandler(NetworkContext networkContext) {

        }

        @Override
        protected void onRead(@NotNull DocumentContext in, @NotNull WireOut out) {
            td.read(in.wire());
            td.write(outWire);
        }

        @Override
        protected void onInitialize() {
        }
    }
}