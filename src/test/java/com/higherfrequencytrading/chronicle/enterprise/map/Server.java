package com.higherfrequencytrading.chronicle.enterprise.map;

import net.openhft.chronicle.network.Network;
import net.openhft.chronicle.network.NioCallback;
import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.chronicle.network.internal.NetworkHub;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static net.openhft.chronicle.network.internal.NetworkConfig.port;

/**
 * @author Rob Austin.
 */
public class Server {

    public static void main(String... args) throws IOException, InterruptedException {

        int bufferSize = 64 * 1024;
        final NetworkConfig echoConf = port(9566).name("ping").tcpBufferSize(10 * bufferSize);
        final CountDownLatch finished = new CountDownLatch(1);

        try (NetworkHub ignored = Network.of(echoConf, withActions -> (in, out, eventType) -> {

                    if (eventType == NioCallback.EventType.CLOSED) {
                        finished.countDown();
                        return;
                    }


                    while (out.remaining() > 0 && in.remaining() > 0) {
                        try {
                            out.writeByte(in.readByte());
                        } catch (IndexOutOfBoundsException e) {
                            out.writeByte(in.readByte());
                        }
                    }


                    //in.position(in.limit());

                }

        )) {
            // auto close
            finished.await();
        }
    }
}
