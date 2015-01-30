/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.performance.tests.vanilla.tcp;

import net.openhft.affinity.AffinitySupport;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author peter.lawrey
 */
public class EchoServerMain {
    public static void main(String... args) throws IOException {
        int port = args.length < 1 ? EchoClientMain.PORT : Integer.parseInt(args[0]);
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(port));
        System.out.println("listening on " + ssc);
        ReentrantLock lock = new ReentrantLock();
        while (true) {
            final SocketChannel socket = ssc.accept();
            socket.socket().setTcpNoDelay(true);
            socket.configureBlocking(false);
            new Thread(() -> {
                boolean locked = lock.tryLock();
                if (locked)
                    AffinitySupport.setAffinity(1 << 2L);
                try {
                    System.out.println("Connected " + socket);
                    // simulate copying the data. 
                    // obviously faster if you don't touch the data but no real service would do that.
                    ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024);
                    ByteBuffer bb2 = ByteBuffer.allocateDirect(256 * 1024);
                    while (socket.read(bb) >= 0) {
                        bb.flip();
                        bb2.put(bb);
                        bb2.flip();
                        // make sure there is enough space to do a full read the next time.
                        if (socket.write(bb2) < 0)
                            throw new EOFException();
                        while (freeSpace(bb2) < bb.capacity()) {
                            System.out.println("Write blocking");
                            if (socket.write(bb2) < 0)
                                throw new EOFException();
                        }

                        if (bb2.remaining() > 0)
                            bb2.compact();
                        else
                            bb2.clear();
                        bb.clear();
                    }
                } catch (IOException ignored) {
                } finally {
                    System.out.println("... disconnected " + socket);
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                    }
                    if (locked)
                        lock.unlock();
                }
            }).start();
        }
    }

    static int freeSpace(ByteBuffer bb2) {
        return bb2.capacity() - bb2.remaining();
    }
}
