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

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class VerySimpleClient {

    public static final WireType WIRE_TYPE = WireType.BINARY;
    final Wire outWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());
    final Wire inWire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());
    long tid = 0;
    private EventLoop eg;
    private String expectedMessage;
    private SocketChannel client;

    /*
     * And, check the benchmark went fine afterwards:
     */
    public static void main(String[] args) throws Exception {
        if (Jvm.isDebug()) {
            VerySimpleClient main = new VerySimpleClient();
            main.setUp();
            for (Method m : VerySimpleClient.class.getMethods()) {
                if (m.getAnnotation(Benchmark.class) != null) {
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 100; j++) {
                            m.invoke(main);

                        }
                    }
                }
            }
            main.tearDown();
        } else {
            int time = Boolean.getBoolean("longTest") ? 30 : 2;

            Options opt = new OptionsBuilder()
                    .include(VerySimpleClient.class.getSimpleName())
                    .warmupIterations(5)
//                .measurementIterations(5)
                    .forks(1)
                    .mode(Mode.SampleTime)
                    .measurementTime(TimeValue.seconds(time))
                    .timeUnit(TimeUnit.NANOSECONDS)
                    .build();

            new Runner(opt).run();
        }
    }

    @Setup
    public void setUp() throws Exception {
        String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        eg = new EventGroup(true);
        eg.start();
        expectedMessage = "<my message>";
        createServer(desc, eg);
        client = createClient(eg, desc);

    }

    @TearDown
    public void tearDown() throws IOException {
        System.out.println("closing");
        eg.stop();

        TCPRegistry.reset();
        client.close();
        client.socket().close();

    }

    @Benchmark
    public String test() throws IOException {

        // create the message to send§
        tid++;
        outWire.clear();
        inWire.clear();
        ((ByteBuffer) inWire.bytes().underlyingObject()).clear();
        ((ByteBuffer) outWire.bytes().underlyingObject()).clear();

        outWire.writeDocument(true, w -> w.write(() -> "tid").int64(tid));
        outWire.writeDocument(false, w -> w.write(() -> "payload").text(expectedMessage));

        final ByteBuffer outBuff = (ByteBuffer) outWire.bytes().underlyingObject();
        outBuff.limit((int) outWire.bytes().writePosition());

        // write the data to the socket
        while (outBuff.hasRemaining())
            client.write(outBuff);

        // meta data
        readDocument(inWire);

        // data
        readDocument(inWire);

        //    System.out.println(Wires.fromSizePrefixedBlobs(inWire.bytes()));

        String[] text = {null};
        // read the reply and check the result
        inWire.readDocument(null, data -> {
            text[0] = data.read(() -> "payloadResponse").text();
        });
        return text[0];
    }

    private void readDocument(Wire inMetaDataWire) throws IOException {
        ByteBuffer inBuff = (ByteBuffer) inMetaDataWire.bytes().underlyingObject();

        // write the data to the socket
        long start = inMetaDataWire.bytes().writePosition();

        while (inBuff.position() < 4 + start)
            client.read(inBuff);

        int len = Wires.lengthOf(inMetaDataWire.bytes().readInt(start));
        inMetaDataWire.bytes().readLimit(start + 4 + len);
        while (inBuff.position() < 4 + len + start)
            client.read(inBuff);
    }

    @NotNull
    private SocketChannel createClient(EventLoop eg, String desc) throws Exception {

        Exception e = null;
        for (int i = 0; i < 10; i++) {

            SocketChannel result = null;
            try {

                result = TCPRegistry.createSocketChannel(desc);
                if (result == null)
                    continue;
                int tcpBufferSize = 2 << 20;
                Socket socket = result.socket();
                socket.setTcpNoDelay(true);
                socket.setReceiveBufferSize(tcpBufferSize);
                socket.setSendBufferSize(tcpBufferSize);
                result.configureBlocking(false);
                return result;

            } catch (IOException e0) {
                e = e0;
                continue;
            }
        }

        throw e;
    }

    private void createServer(String desc, EventLoop eg) throws IOException {
        AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                () -> new WireEchoRequestHandler(WIRE_TYPE), VanillaSessionDetails::new, 0, 0);
        eg.addHandler(eah);
        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
