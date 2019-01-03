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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.core.Jvm.pause;

/**
 * The simple client, which sends a message to the echo server and waits for response
 */
public class GrizzlyClientThroughPutTest {

    static final String DEFAULT_PORT = Integer.toString(GrizzlyEchoServer.PORT);
    static final int PORT = Integer.parseInt(System.getProperty("port", DEFAULT_PORT));
    static final String HOST = System.getProperty("host", "127.0.0.1");

    final static MemoryManager GRIZZLY_MM =
            MemoryManager.DEFAULT_MEMORY_MANAGER;

    public static void main(String[] args) throws IOException,
            ExecutionException, InterruptedException, TimeoutException {

        System.out.println("Starting Grizzly throughput test");

        final int bufferSize = 64 * 1024;
        @NotNull final byte[] payload = new byte[bufferSize];

        final Buffer buffer = GRIZZLY_MM.allocate(bufferSize);
        {
            Arrays.fill(payload, (byte) 'X');
        }

        @Nullable Connection connection = null;

        // Create a FilterChain using FilterChainBuilder
        @NotNull FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        final long startTime = System.nanoTime();
        @NotNull final AtomicLong bytesReceived = new AtomicLong();

        filterChainBuilder.add(new BaseFilter() {
            final Buffer buffer2 = GRIZZLY_MM.allocate(bufferSize);
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

                bytesReceived.addAndGet((ctx.<Buffer>getMessage()).remaining());

                if (i++ % 10000 == 0)
                    System.out.print(".");

                buffer2.clear();
                buffer2.put(payload);
                buffer2.flip();

                final Object peerAddress = ctx.getAddress();
                ctx.write(peerAddress, buffer2, null);
                return ctx.getStopAction();
            }
        });

        // Create TCP transport
        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();

        transport.setProcessor(filterChainBuilder.build());

        try {
            // start the transport
            transport.start();

            // wait for connect operation to complete
            connection = transport.connect(HOST, PORT).get(10, TimeUnit.SECONDS);

            assert connection != null;

            buffer.clear();
            buffer.put(payload);
            buffer.flip();
            connection.write(buffer);

            // the test should run for 10 seconds
            pause(10_000);

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
