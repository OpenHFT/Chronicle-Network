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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BufferSizeTest extends NetworkTestCommon {
    private static final @NotNull
    String desc = "host.port";
    private EventLoop eg;
    private ThreadDump threadDump;

    @BeforeEach
    protected void threadDump() {
        threadDump = new ThreadDump();
    }

    @AfterEach
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @BeforeEach
    void setUp() throws IOException {
        TCPRegistry.createServerSocketChannelFor(desc);
        eg = EventGroup.builder().build();
        eg.start();
        createServer(desc, eg);
    }

    @AfterEach
    public void tearDown() {
        closeQuietly(eg);
        TCPRegistry.reset();
    }

    @Test
    void test() throws IOException {
        sendAndReceive(64 << 10);
    }

    private void sendAndReceive(int tcpBufferSize) throws IOException {
        for (int length = 1; length < 2000; length++)
            sendAndReceive(length, tcpBufferSize);
    }

    private void sendAndReceive(int length, int tcpBufferSize) throws IOException {
        String expectedMessage = "";
        for (int i = 1; i <= length; i++)
            expectedMessage += (char) (32 + (i % (126 - 32)));//(char)('0' + i % 10);

        sendAndReceive(expectedMessage, tcpBufferSize);
    }

    private void sendAndReceive(String expectedMessage, int tcpBufferSize) throws IOException {
        final ChronicleSocketChannel client = createClient(desc, tcpBufferSize);

        assert System.getProperty("TcpEventHandler.tcpBufferSize") == null;
        System.setProperty("TcpEventHandler.tcpBufferSize", Integer.toString(tcpBufferSize));
        Bytes<?> inBytes = null;

        Bytes<?> outBytes = null;
        try {
            outBytes = Bytes.elasticByteBuffer().writeUtf8(expectedMessage);
            final long totalBytes = outBytes.writePosition();
            final ByteBuffer outBuff = (ByteBuffer) outBytes.underlyingObject();

            outBuff.clear();
            outBuff.limit((int) outBytes.writePosition());

            // write the data to the socket
            while (outBuff.hasRemaining())
                client.write(outBuff);

            inBytes = Bytes.elasticByteBuffer((int) totalBytes).clear();
            final ByteBuffer inBuff = (ByteBuffer) inBytes.underlyingObject();

            // read back
            int totalRead = 0;
            int read;
            int count = 0;
            while (totalRead < totalBytes && (read = client.read(inBuff)) > -1) {
                assert read != 0;
                totalRead += read;
                ++count;
            }
            if (count > 1) {
                Jvm.startup().on(BufferSizeTest.class, "count=" + count);
            }

            inBytes.readLimit(totalRead);
            assertEquals(expectedMessage, inBytes.readUtf8());

        } finally {
            inBytes.releaseLast();
            outBytes.releaseLast();
            System.clearProperty("TcpEventHandler.tcpBufferSize");
            client.close();
        }
    }

    @NotNull
    private ChronicleSocketChannel createClient(@NotNull String desc, int tcpBufferSize) throws IOException {

        ChronicleSocketChannel result = TCPRegistry.createSocketChannel(desc);
        ChronicleSocket socket = result.socket();
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(tcpBufferSize);
        socket.setSendBufferSize(tcpBufferSize);
        result.configureBlocking(true);
        return result;
    }

    private <T extends VanillaNetworkContext<T>> void createServer(@NotNull String desc, @NotNull EventLoop eg) throws IOException {
        @NotNull AcceptorEventHandler<T> eah = new AcceptorEventHandler<T>(desc,
                (networkContext) -> {
                    @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);
                    handler.tcpHandler(new EchoHandler<>());
                    return handler;
                },
                () -> (T) new VanillaNetworkContext());

        eg.addHandler(eah);
    }
}
