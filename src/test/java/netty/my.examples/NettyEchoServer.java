package netty.my.examples;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import vanilla.java.tcp.EchoClientMain;

/**
 * Discards any incoming data.
 */
public class NettyEchoServer {

    private int port;

    public NettyEchoServer(int port) {
        this.port = port;
    }


    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // (3)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                                // log out response
                              /*  @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
                                    ByteBuf in = (ByteBuf) msg;
                                    try {
                                        while (in.isReadable()) { // (1)
                                            System.out.print((char) in.readByte());
                                            System.out.flush();
                                        }
                                    } finally {
                                        ReferenceCountUtil.release(msg); // (2)
                                    }
                                }*/


                                // echo server
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
                                    ctx.write(msg); // (1)
                                    ctx.flush(); // (2)
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
                                    // Close the connection when an exception is raised.
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = EchoClientMain.PORT;
        }
        new NettyEchoServer(port).run();
    }
}