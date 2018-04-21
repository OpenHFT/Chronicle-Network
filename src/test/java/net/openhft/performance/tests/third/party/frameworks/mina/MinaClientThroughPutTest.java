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

package net.openhft.performance.tests.third.party.frameworks.mina;

/**
 * @author Rob Austin.
 */

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.Jvm.pause;

public class MinaClientThroughPutTest {

    private static final String DEFAULT_PORT = Integer.toString(MinaEchoServer.PORT);
    private static final int PORT = Integer.parseInt(System.getProperty("port", DEFAULT_PORT));
    private static final String HOST = System.getProperty("host", "127.0.0.1");
    private static final long CONNECT_TIMEOUT = 30 * 1000L; // 30 seconds

    public static void main(String[] args) throws Throwable {

        @NotNull final NioSocketConnector connector = new NioSocketConnector();
        final IoBuffer ioBuffer = IoBuffer.allocate(1024);
        connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

        connector.setHandler(new IoHandlerAdapter() {
            final int bufferSize = 64;
            int bytesReceived = 0;
            long startTime;
            @NotNull
            byte[] payload = new byte[bufferSize];
            int i;

            {
                Arrays.fill(payload, (byte) 'X');
            }

            @Override
            public void sessionOpened(@NotNull IoSession session) {
                startTime = System.nanoTime();
                ioBuffer.clear();
                ioBuffer.put(payload);

                session.write(ioBuffer);
            }

            @Override
            public void sessionClosed(IoSession session) {
            }

            @Override
            public void messageReceived(@NotNull IoSession session, @NotNull Object message) {
                bytesReceived += ((IoBuffer) message).remaining();
                ((IoBuffer) message).clear();

                if (i++ % 10000 == 0)
                    System.out.print(".");

                ((IoBuffer) message).put(payload);
                session.write(message);

                if (TimeUnit.NANOSECONDS.toSeconds(System
                        .nanoTime() - startTime) >= 10) {
                    long time = System.nanoTime() - startTime;
                    System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                            bytesReceived / time);
                    session.close(true);
                }
            }

            @Override
            public void messageSent(IoSession session, Object message) {
            }

            @Override
            public void exceptionCaught(@NotNull IoSession session, @NotNull Throwable cause) {
                cause.printStackTrace();
                session.close(true);
            }

        });

        IoSession session;

        for (; ; ) {
            try {
                ConnectFuture future = connector.connect(new InetSocketAddress(HOST, PORT));
                future.awaitUninterruptibly();
                session = future.getSession();
                break;
            } catch (RuntimeIoException e) {
                e.printStackTrace();
                pause(500);
            }
        }

        // wait until the summation is done
        session.getCloseFuture().awaitUninterruptibly();
        connector.dispose();
    }
}