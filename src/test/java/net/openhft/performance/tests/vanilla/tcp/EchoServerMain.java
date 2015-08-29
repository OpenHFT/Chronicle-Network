/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.affinity.Affinity;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author peter.lawrey
 */
public class EchoServerMain {
    public static void main(@NotNull String... args) throws IOException {
        int port = args.length < 1 ? EchoClientMain.PORT : Integer.parseInt(args[0]);
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(port));
        System.out.println("listening on " + ssc);

        AtomicReference<SocketChannel> nextSocket = new AtomicReference<>();

        new Thread(() -> {
            Affinity.setAffinity(3);

            ByteBuffer bb = ByteBuffer.allocateDirect(32 * 1024);
            ByteBuffer bb2 = ByteBuffer.allocateDirect(32 * 1024);
            List<SocketChannel> sockets = new ArrayList<>();
            for (; ; ) {
                if (sockets.isEmpty())
                    Thread.yield();
                SocketChannel sc = nextSocket.getAndSet(null);
                if (sc != null) {
                    System.out.println("Connected " + sc);
                    sockets.add(sc);
                }
                for (int i = 0; i < sockets.size(); i++) {
                    SocketChannel socket = sockets.get(i);
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
                        try {
                            socket.close();
                        } catch (IOException e) {
                            // ignored
                        }
                        sockets.remove(i--);

                    }
                }
            }
        }).start();

        while (true) {
            final SocketChannel socket = ssc.accept();
            socket.socket().setTcpNoDelay(true);
            socket.configureBlocking(false);
            while (!nextSocket.compareAndSet(null, socket)) {
                // busy wait.
            }
        }
    }
}
