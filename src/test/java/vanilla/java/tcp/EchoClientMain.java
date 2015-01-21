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

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

/**
 * @author peter.lawrey
 */
/*
On a E5-2650 v2 over loopback with onload
Throughput was 2872.7 MB/s
Loop back echo latency was 11.8/12.4 24.6/25.7 30.3us for 50/90 99/99.9 99.99%tile
*/


public class EchoClientMain {
    static final int PORT = 54321;

    public static void main(String... args) throws IOException {
        AffinitySupport.setAffinity(1L << 3);
        String hostname = args[0];
        int port = args.length < 2 ? PORT : Integer.parseInt(args[1]);
        int repeats = 1;

        SocketChannel[] sockets = new SocketChannel[repeats];
        for (int j = 0; j < repeats; j++) {
            sockets[j] = SocketChannel.open(new InetSocketAddress(hostname, port));
            sockets[j].socket().setTcpNoDelay(true);
        }
        testThroughput( sockets);
        testLatency( sockets);
        for (Closeable socket : sockets)
            socket.close();
    }

    private static void testThroughput( SocketChannel[] sockets) throws IOException {
        System.out.println("Starting throughput test");
        int bufferSize = 32 * 1024;
        ByteBuffer bb = ByteBuffer.allocateDirect(bufferSize);
        int count = 0, window = 2;
        long start = System.nanoTime();
        while (System.nanoTime() - start < 5e9) {
            for (SocketChannel socket : sockets) {
                bb.clear();
                if (socket.write(bb) < 0)
                    throw new AssertionError("Socket "+socket+" unable to write in one go.");
            }
            if (count >= window)
                for (SocketChannel socket : sockets) {
                    bb.clear();
                    while(socket.read(bb) > 0 && bb.remaining() > 0);
                }
            count++;
        }
        for (int end = 0; end < Math.min(count, window); end++)
            for (SocketChannel socket : sockets) {
                bb.clear();
                while(socket.read(bb) > 0 && bb.remaining() > 0);
            }
        long time = System.nanoTime() - start;
        System.out.printf("Throughput was %.1f MB/s%n", 1e3 * count * bufferSize * sockets.length / time);
    }

    private static void testLatency(SocketChannel[] sockets) throws IOException {
        System.out.println("Starting latency test");
        int tests = 200000;
        long[] times = new long[tests * sockets.length];
        int count = 0;
        ByteBuffer bb = ByteBuffer.allocateDirect(64);
        for (int i = -20000; i < tests; i++) {
            long now = System.nanoTime();
            for (SocketChannel socket : sockets) {
                bb.clear();
                socket.write(bb);
                if (bb.remaining() > 0)
                    throw new AssertionError("Unable to write in one go.");
            }
            for (SocketChannel socket : sockets) {
                bb.clear();
                socket.read(bb);
                if (bb.remaining() > 0)
                    throw new AssertionError("Unable to read in one go.");
                if (i >= 0)
                    times[count++] = System.nanoTime() - now;
            }
        }
        Arrays.sort(times);
        System.out.printf("Loop back echo latency was %.1f/%.1f %.1f/%.1f %.1fus for 50/90 99/99.9 99.99%%tile%n",
                times[tests / 2] / 1e3, times[tests * 9 / 10] / 1e3,
                times[tests - tests / 100] / 1e3, times[tests - tests / 1000] / 1e3,
                times[tests - tests / 10000] / 1e3);
    }
}
