package net.openhft.performance.tests.third.party.frameworks.grizzly;

import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Class initializes and starts the echo server, based on Grizzly 2.3
 */
public class GrizzlyEchoServer {

    static final Logger LOG = Logger.getLogger(GrizzlyEchoServer.class.getName());
    static final int PORT = Integer.parseInt(System.getProperty("port", "9124"));

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        // Create a FilterChain using FilterChainBuilder
        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        // EchoFilter is responsible for echoing received messages
        filterChainBuilder.add(new BaseFilter() {
            /**
             * Handle just read operation, when some message has come and ready to be
             * processed.
             *
             * @param ctx Context of {@link org.glassfish.grizzly.filterchain.FilterChainContext} processing
             * @return the next action
             * @throws java.io.IOException
             */
            @Override
            public NextAction handleRead(FilterChainContext ctx)
                    throws IOException {
                // Peer address is used for non-connected UDP Connection :)
                final Object peerAddress = ctx.getAddress();
                final Object message = ctx.getMessage();

                ctx.write(peerAddress, message, null);

                return ctx.getStopAction();

            }
        });

        // Create TCP transport
        final TCPNIOTransport transport =
                TCPNIOTransportBuilder.newInstance().build();

        transport.setProcessor(filterChainBuilder.build());
        try {
            // binding transport to start listen on certain host and port
            transport.bind(PORT);

            // start the transport
            transport.start();

            countDownLatch.await(50, TimeUnit.SECONDS);

        } finally {
            LOG.info("Stopping transport...");
            // stop the transport
            transport.shutdownNow();

            LOG.info("Stopped transport...");
        }


    }
}
