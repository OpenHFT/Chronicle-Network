/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    static final int PORT = Integer.parseInt(System.getProperty("port", "9124"));
    private static final Logger LOG = Logger.getLogger(GrizzlyEchoServer.class.getName());
    @NotNull
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        // Create a FilterChain using FilterChainBuilder
        @NotNull FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();

        // Add TransportFilter, which is responsible
        // for reading and writing data to the connection
        filterChainBuilder.add(new TransportFilter());

        // EchoFilter is responsible for echoing received messages
        filterChainBuilder.add(new BaseFilter() {
            @Override
            public NextAction handleRead(@NotNull FilterChainContext ctx) {
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
