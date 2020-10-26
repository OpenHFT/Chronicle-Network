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
package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketFactory;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author peter.lawrey
 */
public class EchoServerMain {
    public static void main(@NotNull String... args) throws IOException {
        int port = args.length < 1 ? EchoClientMain.PORT : Integer.parseInt(args[0]);
        ChronicleServerSocketChannel ssc = ChronicleServerSocketFactory.open();
        ssc.bind(new InetSocketAddress(port));
        System.out.println("listening on " + ssc);

        @NotNull AtomicReference<ChronicleSocketChannel> nextSocket = new AtomicReference<>();

        new Thread(() -> {
            Affinity.acquireCore();
            System.out.println("Running on CPU " + Affinity.getCpu());

            ByteBuffer bb = ByteBuffer.allocateDirect(32 * 1024);
            ByteBuffer bb2 = ByteBuffer.allocateDirect(32 * 1024);
            @NotNull List<ChronicleSocketChannel> sockets = new ArrayList<>();
            for (; ; ) {
                if (sockets.isEmpty())
                    Thread.yield();
                ChronicleSocketChannel sc = nextSocket.getAndSet(null);
                if (sc != null) {
//                    System.out.println("Connected " + sc);
                    sockets.add(sc);
                }
                for (int i = 0; i < sockets.size(); i++) {
                    ChronicleSocketChannel socket = sockets.get(i);
                    try {
                        // simulate copying the data.
                        // obviously faster if you don't touch the data but no real service would do that.
                        bb.clear();
                        int len = socket.read(bb);
                        if (len < 0) {
                            System.out.println("... closed " + socket + " on read");
                            socket.close();
                            sockets.remove(i--);
                            continue;
                        } else if (len == 0) {
                            continue;
                        }
                        bb.flip();
                        bb2.clear();
                        bb2.put(bb);
                        bb2.flip();
                        // make sure there is enough space to do a full read the next time.

                        while ((len = socket.write(bb2)) > 0) {
                            // busy wait
                        }
                        if (len < 0) {
                            System.out.println("... closed " + socket + " on write");
                            socket.close();
                            sockets.remove(i--);
                            continue;
                        }
                    } catch (IOException ioe) {
                        System.out.println("... closed " + socket + " on " + ioe);
                        socket.close();
                        sockets.remove(i--);

                    }
                }
            }
        }).start();

        while (true) {
            final ChronicleSocketChannel socket = ssc.accept();
            socket.socket().setTcpNoDelay(true);
            socket.configureBlocking(false);
            while (!nextSocket.compareAndSet(null, socket)) {
                // busy wait.
            }
        }
    }
}
