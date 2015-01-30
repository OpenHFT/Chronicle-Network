package net.openhft.performance.tests.third.party.frameworks.grizzly;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

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
        final byte[] payload = new byte[bufferSize];

        final Buffer buffer = GRIZZLY_MM.allocate(bufferSize);
        {
            Arrays.fill(payload, (byte) 'X');
        }

        Connection connection = null;

        // Create a FilterChain using FilterChainBuilder
        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        final long startTime = System.nanoTime();
        final AtomicLong bytesReceived = new AtomicLong();


        filterChainBuilder.add(new BaseFilter() {

            int i;
            final Buffer buffer2 = GRIZZLY_MM.allocate(bufferSize);

            /**
             * Handle just read operation, when some message has come and ready to be
             * processed.
             *
             * @param ctx Context of {@link org.glassfish.grizzly.filterchain.FilterChainContext} processing
             * @return the next action
             * @throws java.io.IOException
             */
            @Override
            public NextAction handleRead(final FilterChainContext ctx) throws IOException {

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
            Thread.sleep(10_000);

            long time = System.nanoTime() - startTime;
            System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                    bytesReceived.get() / time);

        } finally

        {
            // close the client connection
            if (connection != null) {
                connection.close();
            }

            // stop the transport
            transport.shutdownNow();
        }
    }
}
