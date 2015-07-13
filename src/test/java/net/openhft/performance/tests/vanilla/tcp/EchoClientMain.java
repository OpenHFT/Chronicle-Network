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

package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.affinity.Affinity;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.EOFException;
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
Throughput was 3259.0 MB/s
Loop back echo latency was 4.7/5.2 6/7 11/97 us for 50/90 99/99.9 99.99/worst %tile

Two connections
Throughput was 3925.0 MB/s
Loop back echo latency was 6.5/7.3 9/10 13/49 us for 50/90 99/99.9 99.99/worst %tile

Four connections
Throughput was 4109.9 MB/s
Loop back echo latency was 10.9/12.2 18/21 33/425 us for 50/90 99/99.9 99.99/worst %tile

Ten connections
Throughput was 2806.6 MB/s
Loop back echo latency was 40.0/44.9 48/57 366/11880 us for 50/90 99/99.9 99.99/worst %tile

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
    public static final int CLIENTS = Integer.getInteger("clients", 1);
    public static final int TARGET_THROUGHPUT = Integer.getInteger("throughput", 20_000);

    public static void main(@NotNull String... args) throws IOException, InterruptedException {
        Affinity.setAffinity(3);
        String[] hostnames = args.length > 0 ? args : "localhost".split(",");

        SocketChannel[] sockets = new SocketChannel[CLIENTS];
//        openConnections(hostnames, PORT, sockets);
//        testThroughput(sockets);
//        closeConnections(sockets);
        openConnections(hostnames, PORT, sockets);
        for (int i : new int[]{100000, 100000, 70000, 50000, 30000, 20000, 10000, 5000})
            testByteLatency(i, sockets);
        closeConnections(sockets);
    }

    private static void openConnections(@NotNull String[] hostname, int port, @NotNull SocketChannel... sockets) throws IOException {
        for (int j = 0; j < sockets.length; j++) {
            sockets[j] = SocketChannel.open(new InetSocketAddress(hostname[j % hostname.length], port));
            sockets[j].socket().setTcpNoDelay(true);
            sockets[j].configureBlocking(false);
        }
    }

    private static void closeConnections(@NotNull SocketChannel... sockets) throws IOException {
        for (Closeable socket : sockets)
            socket.close();
    }

    private static void testThroughput(@NotNull SocketChannel... sockets) throws IOException, InterruptedException {
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

    private static void testLatency(int targetThroughput, @NotNull SocketChannel... sockets) throws IOException {
        System.out.println("Starting latency test rate: " + targetThroughput);
        int tests = Math.min(10 * targetThroughput, 1000000);
        long[] times = new long[tests * sockets.length];
        int count = 0;
        int messageLength = 64;
        ByteBuffer bb = ByteBuffer.allocateDirect(messageLength);
        long now = System.nanoTime();
        long rate = (long) (1e9 / targetThroughput);
        for (int i = -20000; i < tests; i++) {
            now += rate;
            while (System.nanoTime() < now)
                ;
            for (SocketChannel socket : sockets) {
                bb.clear();
                bb.putLong(0, messageLength);
                bb.putLong(8, now);
                int toWrite = bb.remaining();
                int len = socket.write(bb);
                if (len < 0)
                    throw new EOFException();
                if (len < toWrite) {
                    System.out.println("Wrote " + len + " of " + toWrite);
                    socket.write(bb);
                }
                if (bb.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }
            for (SocketChannel socket : sockets) {
                bb.clear();
                while (bb.remaining() > 0)
                    if (socket.read(bb) < 0)
                        throw new AssertionError("Unable to read in one go.");
                if (bb.getLong(0) != messageLength)
                    throw new AssertionError("Not at start of message len " + bb.getLong(0));
                if (bb.getLong(8) != now)
                    throw new AssertionError("Not our message " + bb.getLong(8) + " not " + now);
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

    private static void testByteLatency(int targetThroughput, @NotNull SocketChannel... sockets) throws IOException {
        System.out.println("Starting latency test rate: " + targetThroughput);
        int tests = Math.min(18 * targetThroughput, 1000000);
        long[] times = new long[tests * sockets.length];
        int count = 0;
        long now = System.nanoTime();
        long rate = (long) (1e9 / targetThroughput);

        ByteBuffer bb = ByteBuffer.allocateDirect(4);
        bb.putInt(0, 0x12345678);
        SocketChannel socket = sockets[0];

        for (int i = -20000; i < tests; i++) {
            now += rate;
            while (System.nanoTime() < now)
                ;

            bb.position(0);
            while (bb.remaining() > 0)
                if (socket.write(bb) < 0)
                    throw new EOFException();

            bb.position(0);
            while (bb.remaining() > 0)
                if (socket.read(bb) < 0)
                    throw new EOFException();

            if (bb.getInt(0) != 0x12345678)
                throw new AssertionError("read error");

            if (i >= 0)
                times[count++] = System.nanoTime() - now;
        }
        System.out.println("Average time " + (Arrays.stream(times).sum()/times.length)/1000);
        Arrays.sort(times);
        System.out.printf("Loop back echo latency was %.1f/%.1f %,d/%,d %,d/%d %,d us for 50/90 99/99.9 99.99/99.999 worst %%tile%n",
                times[times.length / 2] / 1e3,
                times[times.length * 9 / 10] / 1e3,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - times.length / 100000] / 1000,
                times[times.length - 1] / 1000
        );
    }
}
