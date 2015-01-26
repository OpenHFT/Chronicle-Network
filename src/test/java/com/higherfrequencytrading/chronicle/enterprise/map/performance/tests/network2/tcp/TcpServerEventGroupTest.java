package com.higherfrequencytrading.chronicle.enterprise.map.performance.tests.network2.tcp;

import net.openhft.chronicle.network2.event.EventGroup;
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

    @Test
    public void testStart() throws Exception {
        EventGroup eg = new EventGroup();
        eg.start();
        AcceptorEventHandler eah = new AcceptorEventHandler(0, EchoHandler::new);
        eg.addHandler(eah);


        SocketChannel[] sc = new SocketChannel[2];
        for (int i = 0; i < sc.length; i++) {
            sc[i] = SocketChannel.open(eah.getLocalAddress());
            sc[i].configureBlocking(false);
        }
        testThroughput(sc);
        testLatency(sc[0]);

        eg.stop();
    }

    private static void testThroughput(SocketChannel... sockets) throws IOException, InterruptedException {
        System.out.println("Starting throughput test");
        int bufferSize = 32 * 1024;
        ByteBuffer bb = ByteBuffer.allocateDirect(bufferSize);
        int count = 1, window = 2;
        long start = System.nanoTime();
        while (System.nanoTime() - start < 10e9) {
            for (SocketChannel socket : sockets) {
                bb.clear();
                bb.putLong(0, count);
//                System.out.println("w1");
                if (socket.write(bb) < 0)
                    throw new AssertionError("Socket " + socket + " unable to write in one go.");
            }
            if (count > window)
                for (SocketChannel socket : sockets) {
                    bb.clear();
//                    System.out.println("r1");
                    while (socket.read(bb) >= 0 && bb.remaining() > 0) ;
                    long ts2 = bb.getLong(0);
                    if (count - window != ts2)
                        assertEquals(count - window, ts2);
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

    private static void testLatency(SocketChannel... sockets) throws IOException {
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
                times[times.length / 2] / 1e3,
                times[times.length * 9 / 10] / 1e3,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - 1] / 1000
        );
    }

}