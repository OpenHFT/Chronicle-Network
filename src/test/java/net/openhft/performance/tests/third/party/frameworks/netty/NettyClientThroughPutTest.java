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
import java.util.concurrent.TimeUnit;

/**
 * Sends one message when a connection is open and echoes back any received data to the server.
 * Simply put, the echo client initiates the ping-pong traffic between the echo client and server by
 * sending the first message to the server.
 */
public final class NettyClientThroughPutTest {

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
        final int bufferSize = 32 * 1024;
        private final ByteBuf firstMessage;
        @NotNull
        byte[] payload = new byte[bufferSize];
        long bytesReceived = 0;
        long startTime;
        int i = 0;

        {
            Arrays.fill(payload, (byte) 'X');
            firstMessage = Unpooled.buffer(bufferSize);
            firstMessage.writeBytes(payload);
        }

        @Override
        public void channelActive(@NotNull ChannelHandlerContext ctx) {
            startTime = System.nanoTime();
            ctx.writeAndFlush(firstMessage);

            System.out.print("Running throughput test ( for 10 seconds ) ");
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) {
            try {
                bytesReceived += ((ByteBuf) msg).readableBytes();

                if (i++ % 10000 == 0)
                    System.out.print(".");
                if (TimeUnit.NANOSECONDS.toSeconds(System
                        .nanoTime() - startTime) >= 10) {
                    long time = System.nanoTime() - startTime;
                    System.out.printf("\nThroughput was %.1f MB/s%n", 1e3 *
                            bytesReceived / time);
                    return;
                }

            } finally {
                ReferenceCountUtil.release(msg); // (2)
            }

            final ByteBuf outMsg = ctx.alloc().buffer(bufferSize); // (2)
            outMsg.writeBytes(payload);

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
