package org.apache.flink.runtime.repartitioning.network

import java.net.SocketAddress

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelFuture, ChannelHandler, ChannelInitializer, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

class RedistributeClient(serverAddr: RedistributorAddress,
                         group: EventLoopGroup,
                         clientHandler: ChannelHandler) {

  def start(): ChannelFuture = {
    val b = new Bootstrap()
    b.group(group)
      .channel(classOf[NioSocketChannel])
      .remoteAddress(serverAddr.value)
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline()
            .addLast(new NettyObjSerializer().encoder)
            .addLast(clientHandler)
        }
      })

    val f = b.connect().sync()
    f
  }

  def stop(): Unit = {
    group.shutdownGracefully().sync()
  }

}
