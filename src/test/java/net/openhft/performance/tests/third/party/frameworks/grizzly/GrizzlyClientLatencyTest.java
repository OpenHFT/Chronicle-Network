package net.openhft.performance.tests.third.party.frameworks.grizzly;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The simple client, which sends a message to the echo server and waits for response
 */
public class GrizzlyClientLatencyTest {

    static final String DEFAULT_PORT = Integer.toString(GrizzlyEchoServer.PORT);
    static final int PORT = Integer.parseInt(System.getProperty("port", DEFAULT_PORT));
    static final String HOST = System.getProperty("host", "127.0.0.1");

    final static MemoryManager GRIZZLY_MM =
            MemoryManager.DEFAULT_MEMORY_MANAGER;


    public static void main(String[] args) throws IOException,
            ExecutionException, InterruptedException, TimeoutException {

        System.out.println("Starting Grizzly latency test");

        final CountDownLatch finished = new CountDownLatch(1);
        final Buffer buffer = GRIZZLY_MM.allocate(8);

        Connection connection = null;

        // Create a FilterChain using FilterChainBuilder
        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        final long startTime = System.nanoTime();
        final AtomicLong bytesReceived = new AtomicLong();


        // StringFilter is responsible for Buffer <-> String conversion
        //    filterChainBuilder.add(new StringFilter(Charset.forName("UTF-8")));
        // ClientFilter is responsible for redirecting server responses to the standard output
        filterChainBuilder.add(new BaseFilter() {

            final long[] times = new long[500_000];
            int count = -50_000; // for warn up - we will skip the first 50_000

            int i;
            final Buffer buffer2 = GRIZZLY_MM.allocate(8);

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

            // perform async. connect to the server
            Future<Connection> future = transport.connect(HOST, PORT);

            // wait for connect operation to complete
            connection = future.get(10, TimeUnit.SECONDS);

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
