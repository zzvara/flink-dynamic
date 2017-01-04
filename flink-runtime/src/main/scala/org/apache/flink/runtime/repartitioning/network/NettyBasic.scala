package org.apache.flink.runtime.repartitioning.network

import io.netty.buffer.ByteBuf;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
  * Handles a server-side channel.
  */
class DiscardServerHandler extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    // Discard the received data silently.
    msg.asInstanceOf[ByteBuf].release()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
  }

}

/**
  * Discards any incoming data.
  */
class DiscardServer(val port: Int) {

  @throws(classOf[Exception])
  def run(): Unit = {
    val bossGroup: EventLoopGroup = new NioEventLoopGroup()
    val workerGroup: EventLoopGroup = new NioEventLoopGroup()

    try {
      val b = new ServerBootstrap(); // (2)
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel]) // (3)
        .childHandler(new ChannelInitializer[SocketChannel]() {
          @throws(classOf[Exception])
          override def initChannel(ch: SocketChannel) {
            ch.pipeline().addLast(new DiscardServerHandler())
          }
        })
        .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128) // (5)
        .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true) // (6)

      // Bind and start to accept incoming connections.
      val f: ChannelFuture = b.bind(port).sync() // (7)

      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
      f.channel().closeFuture().sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

object NettyBasic {

  def main(args: Array[String]): Unit = {
    val port = 12345
    new DiscardServer(port).run()
  }

}
