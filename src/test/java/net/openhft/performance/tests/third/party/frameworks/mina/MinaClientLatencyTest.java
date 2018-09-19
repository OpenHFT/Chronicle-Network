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
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Arrays;

import static net.openhft.chronicle.core.Jvm.pause;

public class MinaClientLatencyTest {

    public static final String DEFAULT_PORT = Integer.toString(MinaEchoServer.PORT);
    static final int PORT = Integer.parseInt(System.getProperty("port", DEFAULT_PORT));

    private static final String HOST = System.getProperty("host", "127.0.0.1");
    private static final long CONNECT_TIMEOUT = 30 * 1000L; // 30 seconds

    public static void main(String[] args) throws Throwable {

        @NotNull final NioSocketConnector connector = new NioSocketConnector();
        @NotNull final long[] times = new long[500_000];
        final int bufferSize = 32 * 1024;

        final IoBuffer ioBuffer = IoBuffer.allocate(bufferSize);
        connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

        connector.setHandler(new IoHandlerAdapter() {
            long startTime;
            int count = -50_000; // for warn up - we will skip the first 50_000
            int i;

            @Override
            public void sessionOpened(@NotNull IoSession session) {
                startTime = System.nanoTime();
                ioBuffer.clear();
                ioBuffer.putLong(System.nanoTime());

                session.write(ioBuffer);
            }

            @Override
            public void sessionClosed(IoSession session) {
            }

            @Override
            public void messageReceived(@NotNull IoSession session, @NotNull Object msg) {
                if (((IoBuffer) msg).remaining() >= 8) {
                    if (count % 10000 == 0)
                        System.out.print(".");

                    if (count >= 0) {
                        times[count] = System.nanoTime() - ((IoBuffer) msg).getLong();

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
                            session.close(true);
                            return;
                        }
                    }

                    count++;
                }

                ioBuffer.clear();
                ioBuffer.putLong(System.nanoTime());

                session.write(ioBuffer); // (3)

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
        @Nullable IoSession session = null;
        try {
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

        } finally {
            if (session != null)
                // wait until the summation is done
                session.getCloseFuture().awaitUninterruptibly();
            connector.dispose();
        }
    }
}