package com.higherfrequencytrading.chronicle.enterprise.map;


import junit.framework.Assert;
import net.openhft.chronicle.network.Network;
import net.openhft.chronicle.network.NioCallback;
import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network.NioCallback.EventType.CONNECT;
import static net.openhft.chronicle.network.internal.NetworkConfig.port;

public class ClientConnectorTest {

    @Test
    public void testPingPong() throws Exception {

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig pingConf = port(9016).name("ping").setEndpoints(new InetSocketAddress("localhost", 9017));

        final NetworkConfig pongConf = port(9017).name("pong");


        try (
                Closeable pong = Network.of(pongConf, withActions -> (in, out, eventType) -> {

                    if (in.remaining() >= "ping".length() + 1) {
                        // 2. when you receive the ping message, send back pong

                        out.writeObject("pong");
                    }
                });

                Closeable ping = Network.of(pingConf,
                        withActions -> (in, out, eventType) -> {

                            switch (eventType) {

                                case CONNECT:
                                    // 1. start by sending a ping message
                                    out.writeObject("ping");
                                    return;

                                case READ:

                                    if (in.remaining() < "pong".length() + 1) {
                                        return;
                                    }


                                    // 3. check the message should be pong
                                    try {
                                        Assert.assertEquals("pong", in.readObject());
                                    } catch (Exception e1) {
                                        e.set(e1);
                                    } finally {

                                        withActions.close();
                                        finished.countDown();
                                    }
                                    out.writeObject("ping");

                            }
                        });

        ) {

            finished.await();
            Exception exception = e.get();

            if (exception != null)
                throw exception;

        }
    }


    @Test
    public void testEchoTroughPutTest() throws Exception {
        int bufferSize = 32 * 1024;
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig echoConf = port(9166).name("echo").tcpBufferSize(100 * bufferSize);

        final NetworkConfig pingConf = port(9167).name("ping")
                .setEndpoints(new InetSocketAddress("localhost", 9166))
                .tcpBufferSize(10 * bufferSize);

        System.out.println("Starting throughput test");

        byte[] bytes = new byte[bufferSize];


        try (
                Closeable echo = Network.of(echoConf, withActions -> (in, out, eventType) -> {

                    if (eventType == NioCallback.EventType.CLOSED)
                        return;

                    while (in.remaining() > 0 && out.remaining() > 0) {
                        if (in.remaining() > 8 && out.remaining() > 8)
                            out.writeLong(in.readLong());
                        else
                            out.writeByte(in.readByte());
                    }

                });

                Closeable ping = Network.of(pingConf, withActions -> new NioCallback() {

                            long start;
                            long bytesRead;

                            int i = 0;

                            @Override
                            public void onEvent(@NotNull Bytes in, @NotNull Bytes out, @NotNull EventType eventType) {

                                try {

                                    if (eventType == CONNECT) {
                                        // 1. start by sending a ping message
                                        out.writeObject(bytes);
                                        start = System.nanoTime();
                                        return;
                                    }

                                    // read in much as you can

                                    i++;
                                    bytesRead += in.limit();
                                    in.position(in.limit());

                                    if (i % 1000 == 0)
                                        System.out.print(".");


                                    if (System.nanoTime() - start > 5e9) {
                                        long time = System.nanoTime() - start;
                                        System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                                                bytesRead / time);

                                        withActions.close();
                                        finished.countDown();
                                        return;
                                    }

                                    boolean wasWritten = false;

                                    // write as much as you can
                                    while (out.remaining() >= bytes.length) {
                                        wasWritten = true;
                                        out.write(bytes);
                                    }

                                    if (!wasWritten)
                                        withActions.setDirty(true);


                                } catch (Exception e1) {
                                    e.set(e1);
                                    withActions.close();
                                    finished.countDown();
                                }
                            }
                        }

                );
        )

        {

            finished.await();
            Exception exception = e.get();

            if (exception != null)
                throw exception;

        }

    }

    @Test
    public void testEchoLatencyPutTest() throws Exception {


        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();


        final NetworkConfig echoConf = port(9066).name("echo").tcpBufferSize(8);

        final NetworkConfig pingConf = port(9067).name("ping")
                .setEndpoints(new InetSocketAddress("localhost", 9066))
                .tcpBufferSize(8);

        try (
                Closeable echo = Network.of(echoConf, withActions -> (in, out, eventType) -> {
                            if (in.remaining() >= 8 && out.remaining() >= 8)
                                try {
                                    out.writeLong(in.readLong());
                                } catch (Exception e2) {
                                    out.writeLong(in.readLong());
                                }
                        }

                );


                Closeable ping = Network.of(pingConf, withActions -> new NioCallback() {

                    int i = 0;
                    int count = -50_000; // for warn up - we will skip the first 50_000
                    long[] times = new long[500_000];


                    @Override
                    public void onEvent(@NotNull Bytes in, @NotNull Bytes out, @NotNull EventType eventType) {


                        switch (eventType) {
                            case CONNECT:

                                // 1. start by sending a ping message
                                System.out.println("Starting latency test");

                                // this causes the OP_WRITE to be called
                                withActions.setDirty(true);
                                return;

                            case READ:

                                if (in.remaining() >= 8) {

                                    if (count % 10000 == 0)
                                        System.out.print(".");

                                    if (count >= 0) {
                                        times[count] = System.nanoTime() - in.readLong();


                                        if (count == times.length - 1) {
                                            Arrays.sort(times);
                                            System.out.printf("\nLoop back echo latency was %.1f/%.1f %,d/%,d %," +
                                                            "d/%d us for 50/90 99/99.9 99.99/worst %%tile%n",
                                                    times[count / 2] / 1e3,
                                                    times[count * 9 / 10] / 1e3,
                                                    times[count - count / 100] / 1000,
                                                    times[count - count / 1000] / 1000,
                                                    times[count - count / 10000] / 1000,
                                                    times[count - 1] / 1000
                                            );
                                            return;
                                        }
                                    }

                                    count++;
                                }


                                // this will cause the WRITE to be called
                                withActions.setDirty(true);
                                return;

                            case WRITE:
                                if (out.remaining() >= 8)
                                    out.writeLong(System.nanoTime());

                        }
                    }


                })) {
            finished.await();
            // auto close
        }
    }
}



