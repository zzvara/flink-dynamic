package org.apache.flink.runtime.repartitioning.network

import java.net.{InetAddress, InetSocketAddress}

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil

class RedistributeServer(handler: ChannelHandler, networkEnvironment: RedistributionNetworkEnvironment) {

  private val group = networkEnvironment.eventLoopGroup

  private var chFuture: ChannelFuture = null

  @throws(classOf[Exception])
  def bind(): RedistributorAddress = {
    val b: ServerBootstrap = new ServerBootstrap()
    b.group(group)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline()
            .addLast(new NettyObjSerializer().decoder)
            .addLast(handler)
        }
      })
      .option[Integer](ChannelOption.SO_BACKLOG, 128)
      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

    // by 0 binding to a random port
    val addr = new InetSocketAddress(networkEnvironment.hostName, 0)
    val f: ChannelFuture = b.bind(addr).sync()
    chFuture = f

    // returning address
    RedistributorAddress(f.channel().localAddress())
  }

  def stop(): Unit = {
//    chFuture.channel().closeFuture().sync()
    group.shutdownGracefully().sync()
  }
}

class EchoClientHandler extends SimpleChannelInboundHandler[ByteBuf] {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.writeAndFlush(Unpooled.copiedBuffer("YEEEEY", CharsetUtil.UTF_8))
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
    println(s"Client received: ${msg.toString(CharsetUtil.UTF_8)}")
  }

}

object RedistributeServer {
  def main(args: Array[String]): Unit = {
    println(InetAddress.getLocalHost())
//    val server = new RedistributeServer(new EchoServerHandler)
//
//    val addr = server.bind()
//    println(addr)
//
//    val client = new RedistributeClient(addr, new NioEventLoopGroup(), new EchoClientHandler)
//    client.start()
//
//    server.stop()
  }
}
