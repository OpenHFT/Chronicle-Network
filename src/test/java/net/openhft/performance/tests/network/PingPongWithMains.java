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
package net.openhft.performance.tests.network;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static net.openhft.chronicle.wire.WireType.TEXT;

public class PingPongWithMains {

    private static final String serverBinding;
    private static final String clientBinding1;
    private static final String clientBinding2;
    private static final int warmup;
    private static final int iterations;
    private static final int wbsize;
    private static final int payload;
    public static final int SIZE_OF_SIZE = 4;

    private final String serverHostPort = "localhost:8097";

    static {
        Jvm.init();
        serverBinding = System.getProperty("ping_pong.server", "any");
        clientBinding1 = System.getProperty("ping_pong.client1", "any");
        clientBinding2 = System.getProperty("ping_pong.client2", "any");
        warmup = Integer.getInteger("ping_pong.warmup", 40_000);
        iterations = Integer.getInteger("ping_pong.iterations", 400_000);
        wbsize = Integer.getInteger("ping_pong.wbsize", 0);
        payload = Integer.getInteger("ping_pong.payload", 16 << 10); // 32 blows up!!
    }

    private static void testLatency(String desc, @NotNull Function<Bytes, Wire> wireWrapper, @NotNull ChronicleSocketChannel... sockets) throws IOException {
        final @NotNull Histogram[] histo;
        final Wire outWire;
        final Wire inWire;
        final @NotNull TestData td;
        final @NotNull TestData td2;

        try (AffinityLock client1 = AffinityLock.acquireLock(clientBinding1)) {
            // allocate buffers etc on CPU/NUMA region for clientBinding1
            histo = new Histogram[sockets.length];
            histo[0] = new Histogram();

            ByteBuffer out = ByteBuffer.allocateDirect(64 * 1024);
            Bytes outBytes = Bytes.wrapForWrite(out);
            outWire = wireWrapper.apply(outBytes);

            ByteBuffer in = ByteBuffer.allocateDirect(64 * 1024);
            Bytes inBytes = Bytes.wrapForRead(in);
            inWire = wireWrapper.apply(inBytes);

            td = new TestData();
            td.payload.write(new byte[payload]);
            td2 = new TestData();
            Jvm.warn().on(PingPongWithMains.class, "Created buffers on core " + client1.cpuId());
        }

        try (AffinityLock client2 = AffinityLock.acquireLock(clientBinding2)) {
            final Bytes workBuffer = Bytes.allocateDirect(wbsize);
            Jvm.warn().on(PingPongWithMains.class, "Running test on " + client2.cpuId());
            for (int i = -warmup; i < iterations; i++) {
                long now = System.nanoTime();
                for (@NotNull ChronicleSocketChannel socket : sockets) {
                    Bytes<ByteBuffer> outBytes = (Bytes<ByteBuffer>) outWire.bytes();
                    ByteBuffer out = outBytes.underlyingObject();
                    out.clear();
                    outBytes.clear();
                    td.value3 = td.value2 = td.value1 = i;
                    try (DocumentContext ignored = outWire.writingDocument(false)) {
                        td.write(outWire);
                    }
                    out.limit((int) outBytes.writePosition());
                    socket.write(out);
                    if (out.remaining() > 0)
                        throw new AssertionError("Unable to write in one go.");
                }

                for (@NotNull ChronicleSocketChannel socket : sockets) {
                    Bytes<ByteBuffer> inBytes = (Bytes<ByteBuffer>) inWire.bytes();
                    ByteBuffer in = inBytes.underlyingObject();
                    in.clear();
                    inBytes.clear();
                    while (true) {
                        int read = socket.read(in);
                        inBytes.readLimit(in.position());
                        if (inBytes.readRemaining() >= SIZE_OF_SIZE) {
                            int header = inBytes.readInt(0);

                            final int len = Wires.lengthOf(header);
                            if (inBytes.readRemaining() >= len) {
                                td2.read(inWire);
                                if (td2.payload.length() != payload)
                                    throw new IllegalStateException();
                            }
                            break;
                        }
                        if (read < 0)
                            throw new AssertionError("Unable to read in one go.");
                    }
                    if (i >= 0)
                        histo[0].sampleNanos(System.nanoTime() - now);
                }

                doSomeMemoryWork(workBuffer);
            }
        }
        inWire.bytes().releaseLast();
        outWire.bytes().releaseLast();

        System.out.printf("%s: Loop back echo latency was %s%n",
                desc,
                histo[0].toMicrosFormat()
        );
    }

    private static void doSomeMemoryWork(Bytes workBuffer) {
        for (int ii = 0; ii < workBuffer.capacity() / 4; ii += 4)
            workBuffer.writeInt(ii, ii);
        for (int ii = 0; ii < workBuffer.capacity() / 4; ii += 4)
            if (ii != workBuffer.readInt(ii))
                throw new IllegalStateException();
    }

    public static void main(String[] args) throws IOException {

        PingPongWithMains instance = new PingPongWithMains();
        if (args.length >= 1 && "client".equals(args[0])) {
            instance.testClient();
        } else {
            instance.testServer();
        }
    }

    private void testClient() throws IOException {

        ChronicleSocketChannel sc = TCPRegistry.createSocketChannel(serverHostPort);
        sc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        sc.configureBlocking(false);

        //       testThroughput(sc);
        testLatency(serverHostPort, WireType.BINARY, sc);

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    public void testServer() throws IOException {
        Jvm.warn().on(PingPongWithMains.class, "Running server on " + serverBinding + " with CIC: " + TcpEventHandler.CREATE_IN_CONSTRUCTOR);
        @NotNull EventLoop eg = EventGroup.builder().withPauser(Pauser.busy()).withBinding(serverBinding).build();

        eg.start();
        TCPRegistry.createServerSocketChannelFor(serverHostPort);
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler(serverHostPort,
                simpleTcpEventHandlerFactory(EchoRequestHandler::new, WireType.BINARY),
                VanillaNetworkContext::new);
        eg.addHandler(eah);
        LockSupport.park();

    }

    private <T extends NetworkContext<T>> Function<T, TcpEventHandler<T>> simpleTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler<T>> defaultHandedFactory, final WireType text) {
        return (networkContext) -> {

            networkContext.wireOutPublisher(new VanillaWireOutPublisher(TEXT));
            @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);
            handler.tcpHandler(new WireTypeSniffingTcpHandler<>(handler,
                    defaultHandedFactory));
            return handler;

        };
    }

    static class EchoRequestHandler extends WireTcpHandler {

        private final TestData td = new TestData();

        public EchoRequestHandler(NetworkContext networkContext) {
        }

        @Override
        protected void onRead(@NotNull DocumentContext in, @NotNull WireOut out) {
            Wire wire = in.wire();
            td.read(wire);
            td.write(outWire);
        }

        @Override
        protected void onInitialize() {
        }
    }
}