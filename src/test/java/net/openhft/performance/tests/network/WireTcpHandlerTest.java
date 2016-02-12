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
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
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
    private final Function<Bytes, Wire> wireWrapper;

    public WireTcpHandlerTest(String desc, Function<Bytes, Wire> wireWrapper) {
        this.desc = desc;
        this.wireWrapper = wireWrapper;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> combinations() {
        return Arrays.asList(
//                new Object[]{"TextWire", WireType.TEXT},
                new Object[]{"BinaryWire", WireType.BINARY},
                new Object[]{"RawWire", WireType.RAW}
        );
    }

    private static void testLatency(String desc, @NotNull Function<Bytes, Wire> wireWrapper, @NotNull SocketChannel... sockets) throws IOException {
//        System.out.println("Starting latency test");
        int tests = 40000;
        long[] times = new long[tests * sockets.length];
        int count = 0;
        ByteBuffer out = ByteBuffer.allocateDirect(64 * 1024);
        Bytes outBytes = Bytes.wrapForWrite(out);
        Wire outWire = wireWrapper.apply(outBytes);

        ByteBuffer in = ByteBuffer.allocateDirect(64 * 1024);
        Bytes inBytes = Bytes.wrapForRead(in);
        Wire inWire = wireWrapper.apply(inBytes);
        TestData td = new TestData();
        TestData td2 = new TestData();
        for (int i = -12000; i < tests; i++) {
            long now = System.nanoTime();
            for (SocketChannel socket : sockets) {
                out.clear();
                outBytes.clear();
                td.value3 = td.value2 = td.value1 = i;
                td.write(outWire);
                out.limit((int) outBytes.writePosition());
                socket.write(out);
                if (out.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }

            for (SocketChannel socket : sockets) {
                in.clear();
                inBytes.clear();
                while (true) {
                    int read = socket.read(in);
                    inBytes.readLimit(in.position());
                    if (inBytes.readRemaining() >= SIZE_OF_SIZE) {
                        long header = inBytes.readInt(0);

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
        EventLoop eg = new EventGroup(true);
        eg.start();
        TCPRegistry.createServerSocketChannelFor(desc);
        AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                () -> new EchoRequestHandler(wireWrapper), VanillaSessionDetails::new, 0, 0);
        eg.addHandler(eah);

        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);

        //       testThroughput(sc);
        testLatency(desc, wireWrapper, sc);

        eg.stop();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

      static class EchoRequestHandler extends WireTcpHandler {
        private final TestData td = new TestData();

          EchoRequestHandler(@NotNull Function<Bytes, Wire> bytesToWire) {
            super(bytesToWire);
        }

        @Override
        protected void process(@NotNull WireIn inWire,
                               @NotNull WireOut outWire,
                               @NotNull SessionDetailsProvider sd) {
            td.read(inWire);
            td.write(outWire);
        }
    }
}