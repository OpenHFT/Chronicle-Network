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

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/*
On an i7-3970X

Throughput was 2944.9 MB/s
Loop back echo latency was 5.0/6.5 8/10 36/2248 us for 50/90 99/99.9 99.99/worst %tile

compared with raw performance of

Throughput was 3728.4 MB/s
Loop back echo latency was 4.8/5.2 5.6/7.4 9.6us for 50/90 99/99.9 99.99%tile
 */

public class TcpServerEventGroupTest {
    private ThreadDump threadDump;

    public static void main(String[] args) throws IOException, InterruptedException {
        new TcpServerEventGroupTest().testStart();
    }

    private static void testThroughput(@NotNull SocketChannel... sockets) throws IOException {
        System.out.println("Starting throughput test");
        int bufferSize = 32 * 1024;
        ByteBuffer bb = ByteBuffer.allocateDirect(bufferSize);
        int count = 1, window = 0;
        long start = System.nanoTime();
        while (System.nanoTime() - start < 10e9) {
            for (@NotNull SocketChannel socket : sockets) {
                bb.clear();
                bb.putLong(0, count);
                if (socket.write(bb) < 0)
                    throw new AssertionError("Socket " + socket + " unable to write in one go.");
            }
            if (count > window)
                for (@NotNull SocketChannel socket : sockets) {
                    bb.clear();
                    while (socket.read(bb) >= 0 && bb.remaining() > 0) ;
                    long ts2 = bb.getLong(0);
                    if (count - window != ts2)
                        assertEquals(count - window, ts2);
                }
            count++;
        }
        for (@NotNull SocketChannel socket : sockets) {
            try {
                do {
                    bb.clear();
                } while (socket.read(bb) > 0);
            } catch (ClosedChannelException expected) {
            }
        }
        long time = System.nanoTime() - start;
        System.out.printf("Throughput was %.1f MB/s%n", 1e3 * count * bufferSize * sockets.length / time);
    }

    private static void testLatency(@NotNull SocketChannel... sockets) throws IOException {
        System.out.println("Starting latency test");
        int tests = 500000;
        @NotNull long[] times = new long[tests * sockets.length];
        int count = 0;
        ByteBuffer bb = ByteBuffer.allocateDirect(64);
        for (int i = -50000; i < tests; i++) {
            long now = System.nanoTime();
            for (@NotNull SocketChannel socket : sockets) {
                bb.clear();
                socket.write(bb);
                if (bb.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }
            for (@NotNull SocketChannel socket : sockets) {
                bb.clear();
                while (bb.remaining() > 0)
                    if (socket.read(bb) < 0)
                        throw new AssertionError("Unable to read in one go.");
                if (i >= 0)
                    times[count++] = System.nanoTime() - now;
            }
        }
        Arrays.sort(times);
        System.out.printf("Loop back echo latency was %.1f/%.1f %,d/%,d %,d/%d us for 50/90 99/99.9 99.99/worst %%tile%n",
                times[times.length / 2] / 1e3,
                times[times.length * 9 / 10] / 1e3,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - 1] / 1000
        );
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Ignore("fix JIRA https://higherfrequencytrading.atlassian.net/browse/NET-13")
    @Test
    public void testStart() throws IOException, InterruptedException {
        @NotNull EventLoop eg = new EventGroup(true);
        eg.start();
        TCPRegistry.createServerSocketChannelFor("TcpServerEventGroupTest");
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler("TcpServerEventGroupTest",
                LegacyHanderFactory.legacyTcpEventHandlerFactory(nc -> new EchoHandler()),
                VanillaNetworkContext::new);

        eg.addHandler(eah);

        SocketChannel sc = TCPRegistry.createSocketChannel("TcpServerEventGroupTest");
        sc.configureBlocking(false);

        testThroughput(sc);
        testLatency(sc);

        eg.stop();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }
}