package com.higherfrequencytrading.chronicle.enterprise.map;


import junit.framework.Assert;
import net.openhft.chronicle.network.Network;
import net.openhft.chronicle.network.NioCallback;
import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.chronicle.network.internal.NetworkHub;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network.NioCallback.EventType.*;
import static net.openhft.chronicle.network.internal.NetworkConfig.port;

public class ClientConnectorTest {

    @Test
    public void testPingPong() throws Exception {

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig pingConf = port(9016).name("ping").setEndpoints(new InetSocketAddress("localhost", 9017));

        final NetworkConfig pongConf = port(9017).name("pong");


        try (
                NetworkHub pong = Network.of(pongConf, withActions -> (in, out, eventType) -> {

                    if (in.remaining() >= "ping".length() + 1) {
                        // 2. when you receive the ping message, send back pong

                        out.writeObject("pong");
                    }
                });

                NetworkHub ping = Network.of(pingConf,
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
        int bufferSize = 64 * 1024;
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig echoConf = port(9566).name("echo").tcpBufferSize(300 * bufferSize);

        final NetworkConfig pingConf = port(9567).name("ping")
                .setEndpoints(new InetSocketAddress("localhost", 9566))
                .tcpBufferSize(512 * bufferSize);

        System.out.println("Starting throughput test");

        byte[] bytes = new byte[bufferSize];


        try (
                NetworkHub echo = Network.of(echoConf, withActions -> (in, out, eventType) -> {

                    if (in.remaining() > 0) {

                        while (in.remaining() > 0 && out.remaining() > 0) {
                            if (in.remaining() > 8 && out.remaining() > 8)
                                out.writeLong(in.readLong());
                            else
                                out.writeByte(in.readByte());
                        }
                    }
                });

                NetworkHub ping = Network.of(pingConf, withActions -> new NioCallback() {

                            long start;
                            long count;


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
                                    while (in.remaining() >= bytes.length) {
                                        in.read(bytes);
                                        count++;
                                    }

                                    if (System.nanoTime() - start > 5e9) {
                                        long time = System.nanoTime() - start;
                                        System.out.printf("Throughput was %.1f MB/s%n", 1e3 * count *
                                                bufferSize / time);
                                        finished.countDown();
                                        return;
                                    }

                                    // write as much as you can
                                    while (out.remaining() >= bytes.length)
                                        out.write(bytes);


                                } catch (Exception e1) {
                                    e.set(e1);
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
                NetworkHub echo = Network.of(echoConf, withActions -> (in, out, eventType) -> {
                            if (in.remaining() >= 8 && out.remaining() >= 8)
                                out.writeLong(in.readLong());
                        }

                );


                NetworkHub ping = Network.of(pingConf, withActions -> new NioCallback() {


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
                                    long l = in.readLong();
                                    System.out.println(TimeUnit.NANOSECONDS.toMicros(System
                                            .nanoTime() - l) + "us");
                                }
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



