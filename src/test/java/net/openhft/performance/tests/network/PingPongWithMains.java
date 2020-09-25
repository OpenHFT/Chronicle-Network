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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static net.openhft.performance.tests.network.LegacyHanderFactory.simpleTcpEventHandlerFactory;

public class PingPongWithMains {

    public static final int SIZE_OF_SIZE = 4;

    private final String serverHostPort = "localhost:8097"; // localhost:8080

    private static void testLatency(String desc, @NotNull Function<Bytes, Wire> wireWrapper, @NotNull ChronicleSocketChannel... sockets) throws IOException {
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
            for (@NotNull ChronicleSocketChannel socket : sockets) {
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

            for (@NotNull ChronicleSocketChannel socket : sockets) {
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
        inWire.bytes().releaseLast();
        outWire.bytes().releaseLast();

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

    public static void main(String[] args) throws IOException {

        PingPongWithMains instance = new PingPongWithMains();
        if (args.length >= 1 && "client".equals(args[0])) {
            instance.testClient();
        } else {
            instance.testServer();
        }
    }

    private void testClient() throws IOException {

        ChronicleSocketChannel sc = TCPRegistry.createSocketChannel(serverHostPort);
        sc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        sc.configureBlocking(false);

        //       testThroughput(sc);
        testLatency(serverHostPort, WireType.BINARY, sc);

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    public void testServer() throws IOException {
        @NotNull EventLoop eg = new EventGroup(true, Pauser.busy(), true);

        eg.start();
        TCPRegistry.createServerSocketChannelFor(serverHostPort);
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler(serverHostPort,
                simpleTcpEventHandlerFactory(EchoRequestHandler::new, WireType.BINARY),
                VanillaNetworkContext::new);
        eg.addHandler(eah);
        LockSupport.park();

    }

    static class EchoRequestHandler extends WireTcpHandler {

        private final TestData td = new TestData();

        public EchoRequestHandler(NetworkContext networkContext) {

        }

        @Override
        protected void onRead(@NotNull DocumentContext in, @NotNull WireOut out) {

            Wire wire = in.wire();
            //          System.out.println(wire);
            td.read(wire);
            td.write(outWire);

        }

        @Override
        protected void onInitialize() {
        }
    }
}