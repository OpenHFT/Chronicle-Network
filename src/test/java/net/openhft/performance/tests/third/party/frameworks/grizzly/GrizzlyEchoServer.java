/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.performance.tests.third.party.frameworks.grizzly;

import org.glassfish.grizzly.filterchain.*;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder.newInstance;

/**
 * Class initializes and starts the echo server, based on Grizzly 2.3
 */
public class GrizzlyEchoServer {

    static final Logger LOG = Logger.getLogger(GrizzlyEchoServer.class.getName());
    static final int PORT = Integer.parseInt(System.getProperty("port", "9124"));

    @NotNull
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        // Create a FilterChain using FilterChainBuilder
        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        // EchoFilter is responsible for echoing received messages
        filterChainBuilder.add(new BaseFilter() {
            @Override
            public NextAction handleRead(@NotNull FilterChainContext ctx) throws IOException {
                // Peer address is used for non-connected UDP Connection :)
                final Object peerAddress = ctx.getAddress();
                final Object message = ctx.getMessage();

                ctx.write(peerAddress, message, null);

                return ctx.getStopAction();
            }
        });

        // Create TCP transport
        final TCPNIOTransport transport = newInstance().build();

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
