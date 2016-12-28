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

package net.openhft.performance.tests.third.party.frameworks.grizzly;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The simple client, which sends a message to the echo server and waits for response
 */
public class GrizzlyClientLatencyTest {

    private static final String DEFAULT_PORT = Integer.toString(GrizzlyEchoServer.PORT);
    private static final int PORT = Integer.parseInt(System.getProperty("port", DEFAULT_PORT));
    private static final String HOST = System.getProperty("host", "127.0.0.1");

    public static void main(String[] args) throws IOException,
            ExecutionException, InterruptedException, TimeoutException {

        System.out.println("Starting Grizzly latency test");

        @NotNull final CountDownLatch finished = new CountDownLatch(1);
        final Buffer buffer = MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(8);

        @Nullable Connection connection = null;

        // Create a FilterChain using FilterChainBuilder
        @NotNull FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        final long startTime = System.nanoTime();
        @NotNull final AtomicLong bytesReceived = new AtomicLong();

        filterChainBuilder.add(new BaseFilter() {
            final long[] times = new long[500_000];
            final Buffer buffer2 = MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(8);
            int count = -50_000; // for warn up - we will skip the first 50_000
            int i;

            /**
             * Handle just read operation, when some message has come and ready to be
             * processed.
             *
             * @param ctx Context of {@link org.glassfish.grizzly.filterchain.FilterChainContext} processing
             * @return the next action
             * @throws java.io.IOException
             */
            @Override
            public NextAction handleRead(@NotNull final FilterChainContext ctx) {

                if (i++ % 100_000 == 0)
                    System.out.print(".");

                final Object peerAddress = ctx.getAddress();

                Buffer msg = ctx.<Buffer>getMessage();
                if (msg.remaining() >= 8) {
                    if (count % 10000 == 0)
                        System.out.print(".");

                    if (count >= 0) {
                        times[count] = System.nanoTime() - msg.getLong();

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
                            try {
                                finished.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return ctx.getStopAction();
                        }
                    }

                    count++;
                }

                buffer2.clear();
                buffer2.putLong(System.nanoTime());
                buffer2.flip();

                ctx.write(peerAddress, buffer2, null);
                return ctx.getStopAction();
            }
        });

        // Create TCP transport
        final TCPNIOTransport transport =
                TCPNIOTransportBuilder.newInstance().build();

        transport.setProcessor(filterChainBuilder.build());

        try {
            // start the transport
            transport.start();

            // wait for connect operation to complete
            connection = transport.connect(HOST, PORT).get(10, TimeUnit.SECONDS);

            assert connection != null;

            // send the first message
            buffer.clear();
            buffer.putLong(System.nanoTime());
            buffer.flip();
            connection.write(buffer);

            // wait for the 50_000 messages to be processed
            finished.await();

            long time = System.nanoTime() - startTime;
            System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                    bytesReceived.get() / time);
        } finally {
            // close the client connection
            if (connection != null) {
                connection.close();
            }

            // stop the transport
            transport.shutdownNow();
        }
    }
}
