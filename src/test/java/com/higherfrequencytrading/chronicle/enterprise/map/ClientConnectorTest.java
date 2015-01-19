package com.higherfrequencytrading.chronicle.enterprise.map;


import junit.framework.Assert;
import net.openhft.chronicle.network.Network;
import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.chronicle.network.internal.NetworkHub;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.network.internal.NetworkConfig.port;

public class ClientConnectorTest {

    @Test
    public void test() throws Exception {

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        final NetworkConfig pingConf = port(9026).name("ping");

        final NetworkConfig pongConf = port(9027).name("pong")
                .setEndpoints(new InetSocketAddress("localhost", 9026));

        try (
                NetworkHub pong = Network.of(pingConf, withActions -> (in, out, eventType) -> {

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
                                    withActions.setDirty(true);
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
                                    } finally {
                                        finished.countDown();
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



