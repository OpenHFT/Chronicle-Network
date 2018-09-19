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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class VerySimpleClientTest {

    public static final WireType WIRE_TYPE = WireType.BINARY;
    final Wire outWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());
    final Wire inWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());

    private EventLoop eg;
    private String expectedMessage;
    private SocketChannel client;

    /*
     * And, check the benchmark went fine afterwards:
     */
    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void setUp() throws IOException {
        @NotNull String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        eg = new EventGroup(true);
        eg.start();
        expectedMessage = "<my message>";
        createServer(desc, eg);
        client = createClient(eg, desc);

    }

    @After
    public void tearDown() {
        eg.stop();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test
    public void test() throws IOException {

        // create the message to send§
        final long tid = 0;
        outWire.clear();
        outWire.writeDocument(true, w -> w.write(() -> "tid").int64(tid));
        outWire.writeDocument(false, w -> w.write(() -> "payload").text(expectedMessage));

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
            Assert.assertEquals(expectedMessage, data.read(() -> "payloadResponse").text());
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
    private SocketChannel createClient(EventLoop eg, @NotNull String desc) throws IOException {

        SocketChannel result = TCPRegistry.createSocketChannel(desc);
        int tcpBufferSize = 2 << 20;
        Socket socket = result.socket();
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
        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
