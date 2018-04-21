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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.network.connection.SocketAddressSupplier.uri;

@State(Scope.Thread)
public class ChanelHubTest {

    public static final WireType WIRE_TYPE = WireType.BINARY;
    final Wire wire = WIRE_TYPE.apply(Bytes.elasticByteBuffer());
    private TcpChannelHub tcpChannelHub;
    private EventLoop eg;
    private String expectedMessage;

    public static void main(String[] args) throws RunnerException, InvocationTargetException, IllegalAccessException, IOException {
        if (Jvm.isDebug()) {
            ChanelHubTest main = new ChanelHubTest();
            main.setUp();
            for (Method m : ChanelHubTest.class.getMethods()) {
                if (m.getAnnotation(Benchmark.class) != null) {
                    m.invoke(main);
                }
            }
            main.tearDown();
        } else {
            int time = Boolean.getBoolean("longTest") ? 30 : 2;
            Options opt = new OptionsBuilder()
                    .include(ChanelHubTest.class.getSimpleName())
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

    /*
     * And, check the benchmark went fine afterwards:
     */
    @Setup
    public void setUp() throws IOException {
        String desc = "host.port";
        TCPRegistry.createServerSocketChannelFor(desc);
        eg = new EventGroup(true);
        eg.start();
        expectedMessage = "<my message>";
        createServer(desc, eg);
        tcpChannelHub = createClient(eg, desc);
    }

    @TearDown
    public void tearDown() {
        eg.stop();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        tcpChannelHub.close();
    }

    @Benchmark
    public String test() {

        // create the message to send§
        final long tid = tcpChannelHub.nextUniqueTransaction(System.currentTimeMillis());
        wire.clear();
        wire.writeDocument(true, w -> w.writeEventName(() -> "tid").int64(tid));
        wire.writeDocument(false, w -> w.writeEventName(() -> "payload").text(expectedMessage));

        // write the data to the socket
        tcpChannelHub.lock(() -> tcpChannelHub.writeSocket(wire));

        // read the reply from the socket ( timeout after 1 second )
        Wire reply = tcpChannelHub.proxyReply(TimeUnit.SECONDS.toMillis(1), tid);

        String[] text = {null};
        // read the reply and check the result
        reply.readDocument(null, data -> {
            text[0] = data.readEventName(new StringBuilder()).text();
        });
        return text[0];
    }

    @NotNull
    private TcpChannelHub createClient(EventLoop eg, String desc) {
        return new TcpChannelHub(null, eg, WIRE_TYPE, "", uri(desc), false, null, HandlerPriority.MONITOR);
    }

    private void createServer(String desc, EventLoop eg) throws IOException {
        AcceptorEventHandler eah = new AcceptorEventHandler(desc,
                () -> new WireEchoRequestHandler(WIRE_TYPE), VanillaSessionDetails::new, 0, 0);
        eg.addHandler(eah);
        SocketChannel sc = TCPRegistry.createSocketChannel(desc);
        sc.configureBlocking(false);
    }
}
