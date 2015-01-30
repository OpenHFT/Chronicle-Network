/*
 * Copyright 2014 Higher Frequency Trading
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

package vanilla.java.tcp;

import net.openhft.affinity.AffinitySupport;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

/**
 * @author peter.lawrey
 */
/* Both ends are run with -Xmx64m -verbose:gc

///////// EchoServerMain
On a E5-2650 v2 over loopback with onload
Throughput was 2880.4 MB/s
Loop back echo latency was 5.8/6.2 9.6/19.4 23.2us for 50/90 99/99.9 99.99%tile

On an i7-3970X over loopback
Throughput was 3728.4 MB/s
Loop back echo latency was 4.8/5.2 5.6/7.4 9.6us for 50/90 99/99.9 99.99%tile

Between two servers via Solarflare with onload on server & client (no minor GCs)
Throughput was 1156.0 MB/s
Loop back echo latency was 12.2/12.5 21/25 28/465 us for 50/90 99/99.9 99.99/worst %tile

Between two servers via Solarflare with onload on client (no minor GCs)
Throughput was 867.5 MB/s
Loop back echo latency was 15.0/15.7 21/27 30/398 us for 50/90 99/99.9 99.99/worst %tile

//////// NettyEchoServer
Between two servers via Solarflare with onload on server & client (16 minor GCs)
Throughput was 968.7 MB/s
Loop back echo latency was 18.4/19.0 26/31 33/1236 us for 50/90 99/99.9 99.99/worst %tile

Between two servers via Solarflare with onload on client (16 minor GCs)
Throughput was 643.6 MB/s
Loop back echo latency was 20.8/21.8 29/34 38/2286 us for 50/90 99/99.9 99.99/worst %tile

*/


public class EchoClientMain {
    public static final int PORT = Integer.getInteger("port", 8007);

    public static void main(String... args) throws IOException, InterruptedException {
        AffinitySupport.setAffinity(1L << 3);
        String[] hostnames = args;
        int repeats = args.length;

        SocketChannel[] sockets = new SocketChannel[repeats];
        openConnections(hostnames, PORT, sockets);
        testThroughput(sockets);
        closeConnections(sockets);
        openConnections(hostnames, PORT, sockets);
        testLatency(sockets);
        closeConnections(sockets);
    }

    private static void openConnections(String[] hostnames, int port, SocketChannel[] sockets) throws IOException {
        for (int j = 0; j < sockets.length; j++) {
            sockets[j] = SocketChannel.open(new InetSocketAddress(hostnames[j % hostnames.length], port));
            sockets[j].socket().setTcpNoDelay(true);
            sockets[j].configureBlocking(false);
        }
    }

    private static void closeConnections(SocketChannel[] sockets) throws IOException {
        for (Closeable socket : sockets)
            socket.close();
    }

    private static void testThroughput(SocketChannel[] sockets) throws IOException, InterruptedException {
        System.out.println("Starting throughput test");
        int bufferSize = 32 * 1024;
        ByteBuffer bb = ByteBuffer.allocateDirect(bufferSize);
        int count = 0, window = 2;
        long start = System.nanoTime();
        while (System.nanoTime() - start < 10e9) {
            for (SocketChannel socket : sockets) {
                bb.clear();
                if (socket.write(bb) < 0)
                    throw new AssertionError("Socket " + socket + " unable to write in one go.");
            }
            if (count >= window)
                for (SocketChannel socket : sockets) {
                    bb.clear();
                    while (socket.read(bb) >= 0 && bb.remaining() > 0) ;
                }
            count++;
        }
        for (SocketChannel socket : sockets) {
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

    private static void testLatency(SocketChannel[] sockets) throws IOException {
        System.out.println("Starting latency test");
        int tests = 500000;
        long[] times = new long[tests * sockets.length];
        int count = 0;
        ByteBuffer bb = ByteBuffer.allocateDirect(64);
        for (int i = -50000; i < tests; i++) {
            long now = System.nanoTime();
            for (SocketChannel socket : sockets) {
                bb.clear();
                socket.write(bb);
                if (bb.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }
            for (SocketChannel socket : sockets) {
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
                times[tests / 2] / 1e3,
                times[tests * 9 / 10] / 1e3,
                times[tests - tests / 100] / 1000,
                times[tests - tests / 1000] / 1000,
                times[tests - tests / 10000] / 1000,
                times[tests - 1] / 1000
        );
    }
}
