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
package net.openhft.performance.tests.third.party.frameworks.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import net.openhft.performance.tests.vanilla.tcp.EchoClientMain;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLException;
import java.util.Arrays;

/**
 * Sends one message when a connection is open and echoes back any received data to the server.
 * Simply put, the echo client initiates the ping-pong traffic between the echo client and server by
 * sending the first message to the server.
 */
public final class NettyClientLatencyTest {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", Integer.toString(EchoClientMain
            .PORT)));

    public static void main(String[] args) throws SSLException, InterruptedException {
        // Configure SSL.git
        @Nullable final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);

        } else {
            sslCtx = null;
        }

        // Configure the client.
        @NotNull EventLoopGroup group = new NioEventLoopGroup();
        try {
            @NotNull Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(@NotNull SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                            }
                            //p.addLast(new LoggingHandler(LogLevel.INFO));
                            p.addLast(new MyChannelInboundHandler());
                        }
                    });

            // Start the client.
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    static class MyChannelInboundHandler extends ChannelInboundHandlerAdapter {
        private final ByteBuf firstMessage;

        long startTime;
        int count = -50_000; // for warn up - we will skip the first 50_000
        @NotNull
        long[] times = new long[500_000];

        {
            firstMessage = Unpooled.buffer(8);
            firstMessage.writeLong(System.nanoTime());
        }

        @Override
        public void channelActive(@NotNull ChannelHandlerContext ctx) {
            startTime = System.nanoTime();
            ctx.writeAndFlush(firstMessage);

            System.out.print("Running latency test ( for about 25 seconds ) ");
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) {
            try {

                if (((ByteBuf) msg).readableBytes() >= 8) {
                    if (count % 10000 == 0)
                        System.out.print(".");

                    if (count >= 0) {
                        times[count] = System.nanoTime() - ((ByteBuf) msg).readLong();

                        if (count == times.length - 1) {
                            Arrays.sort(times);
                            System.out.printf("\nLoop back echo latency was %.1f/%.1f %,d/%,d %," +
                                            "d/%d us for 50/90 99/99.9 99.99/worst %%tile%n",
                                    times[count / 2] / 1e3,
                                    times[count * 9 / 10] / 1e3,
                                    times[count - count / 100] / 1000,
                                    times[count - count / 1000] / 1000,
                                    times[count - count / 10000] / 1000,
                                    times[count - 1] / 1000
                            );
                            return;
                        }
                    }

                    count++;
                }
            } finally {
                ReferenceCountUtil.release(msg); // (2)
            }

            final ByteBuf outMsg = ctx.alloc().buffer(8); // (2)
            outMsg.writeLong(System.nanoTime());

            ctx.writeAndFlush(outMsg); // (3)

        }

        @Override
        public void channelReadComplete(@NotNull ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(@NotNull ChannelHandlerContext ctx, @NotNull Throwable cause) {
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close();
        }
    }
}
