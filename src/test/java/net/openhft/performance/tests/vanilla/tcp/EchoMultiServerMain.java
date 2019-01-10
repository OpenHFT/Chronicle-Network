/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author peter.lawrey
 */
public class EchoMultiServerMain {
    public static void main(@NotNull String... args) throws IOException {
        int port = args.length < 1 ? EchoClientMain.PORT : Integer.parseInt(args[0]);
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(port));
        System.out.println("listening on " + ssc);
        ExecutorService service = new ThreadPoolExecutor(0, 1000,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedThreadFactory("connections", true));
        ((ThreadPoolExecutor) service).setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            final SocketChannel socket = ssc.accept();
            socket.socket().setTcpNoDelay(true);
            socket.configureBlocking(true);
            service.submit(() -> process(socket));
        }
    }

    private static void process(SocketChannel socket) {
        ByteBuffer bb = ByteBuffer.allocateDirect(4 * 1024);
        ByteBuffer bb2 = ByteBuffer.allocateDirect(4 * 1024);
        for (; ; ) {
            try {
                // simulate copying the data.
                // obviously faster if you don't touch the data but no real service would do that.
                bb.clear();
                int len = socket.read(bb);
                if (len < 0) {
                    System.out.println("... closed " + socket + " on read");
                    socket.close();
                    return;
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
                    return;
                }
            } catch (IOException ioe) {
                System.out.println("... closed " + socket + " on " + ioe);
                try {
                    socket.close();
                } catch (IOException e) {
                    // ignored
                }
                return;
            }
        }
    }
}
