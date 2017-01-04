package org.apache.flink.runtime.repartitioning.network

import java.net.InetSocketAddress

import io.netty.channel._
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil

@Sharable
class EchoServerHandler extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    val in = msg.asInstanceOf[ByteBuf]
    println(s"Server received: ${in.toString(CharsetUtil.UTF_8)}")
    ctx.write(in)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
      .addListener(ChannelFutureListener.CLOSE)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

}

class EchoServer(val port: Int) {

  @throws(classOf[Exception])
  def start(): Unit = {
    val serverHandler = new EchoServerHandler
    val group = new NioEventLoopGroup()

    try {
      val b: ServerBootstrap = new ServerBootstrap()
      b.group(group)
        .channel(classOf[NioServerSocketChannel])
        .localAddress(new InetSocketAddress(port))
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(serverHandler)
          }
        })

      val f: ChannelFuture = b.bind().sync()
      f.channel().closeFuture().sync()
    } finally {
      group.shutdownGracefully().sync()
    }
  }
}

object NettyBook101 {
  def main(args: Array[String]): Unit = {
    new EchoServer(12345).start()
  }
}
