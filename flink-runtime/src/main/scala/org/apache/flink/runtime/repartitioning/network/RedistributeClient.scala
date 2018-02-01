/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
