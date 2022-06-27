/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package net.openhft.performance.tests.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_USE_PADDING;
import static org.junit.jupiter.api.Assertions.assertEquals;

class VerySimpleClientTest extends NetworkTestCommon {

    public static final WireType WIRE_TYPE = WireType.BINARY;
    final Wire outWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());
    final Wire inWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());

    private EventLoop eg;
    private String expectedMessage;
    private ChronicleSocketChannel client;
    private ChronicleSocketChannel sc;

    public VerySimpleClientTest() {
        outWire.usePadding(TCP_USE_PADDING);
        inWire.usePadding(TCP_USE_PADDING);
    }

    @BeforeEach
    void setUp() throws IOException {
        @NotNull String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        eg = EventGroup.builder().build();
        eg.start();
        expectedMessage = "<my message>";
        createServer(desc, eg);
        client = createClient(eg, desc);

    }

    @AfterEach
    void tearDown() {
        Closeable.closeQuietly(sc, eg, client);
        TcpChannelHub.closeAllHubs();
        inWire.bytes().releaseLast();
        outWire.bytes().releaseLast();
    }

    @Test
    void test() throws IOException {
        // create the message to send§
        final long tid = 0;
        outWire.clear();
        outWire.writeDocument(true, w -> w.write("tid").int64(tid));
        outWire.writeDocument(false, w -> w.write("payload").text(expectedMessage));

        @Nullable final ByteBuffer outBuff = (ByteBuffer) outWire.bytes().underlyingObject();

        outBuff.clear();
        outBuff.limit((int) outWire.bytes().writePosition());

        // write the data to the socket
        while (outBuff.hasRemaining())
            client.write(outBuff);

        // meta data
        readDocument(inWire);

        // data
        readDocument(inWire);

        inWire.readDocument(null, data -> {
            assertEquals(expectedMessage, data.read("payloadResponse").text());
        });

    }

    private void readDocument(@NotNull Wire inMetaDataWire) throws IOException {
        @Nullable ByteBuffer inBuff = (ByteBuffer) inMetaDataWire.bytes().underlyingObject();

        // write the data to the socket
        long start = inMetaDataWire.bytes().readPosition();
        while (inBuff.position() + start < 4)
            client.read(inBuff);

        inMetaDataWire.bytes().writePosition(inBuff.position());
        int len = Wires.lengthOf(inMetaDataWire.bytes().readInt(start));
        while (inBuff.position() < 4 + len + start)
            client.read(inBuff);
    }

    @NotNull
    private ChronicleSocketChannel createClient(EventLoop eg, @NotNull String desc) throws IOException {

        ChronicleSocketChannel result = TCPRegistry.createSocketChannel(desc);
        int tcpBufferSize = 2 << 20;
        ChronicleSocket socket = result.socket();
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        result.configureBlocking(true);
        return result;
    }

    private void createServer(@NotNull String desc, @NotNull EventLoop eg) throws IOException {
        @NotNull AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                LegacyHanderFactory.simpleTcpEventHandlerFactory(
                        WireEchoRequestHandler::new, WIRE_TYPE),
                VanillaNetworkContext::new);

        eg.addHandler(eah);
        sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
