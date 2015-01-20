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
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network.internal.NetworkConfig.port;

public class ClientConnectorTest {

    @Test
    public void testPingPong() throws Exception {

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig pingConf = port(9026).name("ping");

        final NetworkConfig pongConf = port(9027).name("pong")
                .setEndpoints(new InetSocketAddress("localhost", 9026));

        try (
                NetworkHub pong = Network.of(pongConf, withActions -> (in, out, eventType) -> {

                    if (in.remaining() >= "ping".length() + 1) {
                        // 2. when you receive the ping message, send back pong

                        out.writeObject("pong");
                    }
                });

                NetworkHub ping = Network.of(pongConf,
                        withActions -> (in, out, eventType) -> {

                            switch (eventType) {

                                case OP_CONNECT:
                                    // 1. start by sending a ping message
                                    out.writeObject("ping");
                                    return;

                                case OP_READ:

                                    if (in.remaining() < "pong".length() + 1) {
                                        return;
                                    }


                                    // 3. check the message should be pong
                                    try {
                                        Assert.assertEquals("pong", in.readObject());
                                    } catch (Exception e1) {
                                        e.set(e1);
                                        finished.countDown();
                                    } finally {
                                        // finished.countDown();
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

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig echoConf = port(9026).name("ping").tcpBufferSize(256 * 1024);

        final NetworkConfig pingConf = port(9027).name("pong")
                .setEndpoints(new InetSocketAddress("localhost", 9026))
                .tcpBufferSize(256 * 1024);

        System.out.println("Starting throughput test");
        int bufferSize = 128 * 1024;
        byte[] bytes = new byte[bufferSize];


        try (
                NetworkHub echo = Network.of(echoConf, withActions -> (in, out, eventType) -> {

                    if (in.remaining() > 0) {
                        if (out.remaining() < in.remaining()) {
                            // send what you can
                            long limit = in.limit();
                            in.limit(in.position() + out.remaining());
                            out.write(in);
                            in.limit(limit);

                        } else
                            out.write(in);
                    }
                });


                NetworkHub ping = Network.of(pingConf, withActions -> new NioCallback() {

                    long start;
                    long count;

                    @Override
                    public void onEvent(@NotNull Bytes in, @NotNull Bytes out, @NotNull EventType eventType) {


                        switch (eventType) {

                            case OP_CONNECT:
                                // 1. start by sending a ping message
                                out.writeObject(bytes);
                                start = System.nanoTime();
                                return;


                            case OP_READ:

                                in.read(bytes);
                                out.write(bytes);

                                count++;

                                if (System.nanoTime() - start > 5e9) {
                                    long time = System.nanoTime() - start;
                                    System.out.printf("Throughput was %.1f MB/s%n", 1e3 * count *
                                            bufferSize / time);
                                    finished.countDown();
                                }

                        }
                    }
                });
        ) {

            finished.await();
            Exception exception = e.get();

            if (exception != null)
                throw exception;

        }
    }
}



