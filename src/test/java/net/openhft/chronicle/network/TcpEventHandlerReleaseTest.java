/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.api.TcpHandler;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TcpEventHandlerReleaseTest extends NetworkTestCommon {
    private static final String hostPort = "host.port";

    @BeforeEach
    void setUp() throws IOException {
        TCPRegistry.createServerSocketChannelFor(hostPort);
    }

    @Test
    void testRelease() throws IOException {
        try (TcpEventHandler t = createTcpEventHandler()) {
            t.loopFinished();
            t.close();
            // check second close OK
        }
    }

    @Test
    void testBuffersReleasedWhenSocketChannelClosed() throws IOException {
        try (TcpEventHandler t = createTcpEventHandler()) {
            t.socketChannel().close();
            try {
                t.action();
                fail();
            } catch (InvalidEventHandlerException e) {
                // expected.
            } finally {
                t.loopFinished();
            }
        }
    }

    @Test
    void performIdleWorkIsOnlyCalledWhenHandlerIsBusyOrOneHundredIterations() throws IOException, InvalidEventHandlerException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        try (BusyTcpEventHandler tcpEventHandler = new BusyTcpEventHandler(nc)) {
            final BusyTcpHandler tcpHandler = new BusyTcpHandler();
            tcpEventHandler.tcpHandler(tcpHandler);

            // not called when busy
            tcpEventHandler.busy = true;
            tcpEventHandler.action();
            assertEquals(0, tcpHandler.performedIdleWorkCount.get());

            // called when not busy
            tcpEventHandler.busy = false;
            tcpEventHandler.action();
            assertEquals(1, tcpHandler.performedIdleWorkCount.get());

            // called when not called for 101 iterations
            tcpEventHandler.busy = true;
            for (int i = 0; i < 101; i++) {
                tcpEventHandler.action();
            }
            assertEquals(1, tcpHandler.performedIdleWorkCount.get());
            tcpEventHandler.action();
            assertEquals(2, tcpHandler.performedIdleWorkCount.get());
        }
    }

    public TcpEventHandler createTcpEventHandler() throws IOException {
        NetworkContext nc = new VanillaNetworkContext();
        nc.socketChannel(TCPRegistry.createSocketChannel(hostPort));
        TcpEventHandler tcpEventHandler = new TcpEventHandler(nc);
        tcpEventHandler.tcpHandler(NullTcpHandler.INSTANCE);
        return tcpEventHandler;
    }

    enum NullTcpHandler implements TcpHandler {
        INSTANCE;

        @Override
        public void process(@NotNull Bytes in, @NotNull Bytes out, NetworkContext nc) {

        }

        @Override
        public void close() {
        // Do nothing
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    /**
     * This is nasty, but the TcpEventHandler is very hard to test
     */
    private static class BusyTcpEventHandler<N extends NetworkContext<N>> extends TcpEventHandler<N> {

        private boolean busy = true;

        public BusyTcpEventHandler(@NotNull N nc) {
            super(nc, true);
        }

        @Override
        public boolean writeAction() {
            return busy;
        }
    }

    private static class BusyTcpHandler<N extends NetworkContext<N>>  implements TcpHandler<N> {

        private final AtomicInteger performedIdleWorkCount = new AtomicInteger();

        @Override
        public void process(@NotNull Bytes<?> in, @NotNull Bytes<?> out, N nc) {
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void performIdleWork() {
            performedIdleWorkCount.incrementAndGet();
        }
    }
}
