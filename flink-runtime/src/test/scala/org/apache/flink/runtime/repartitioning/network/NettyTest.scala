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

import io.netty.channel.nio.NioEventLoopGroup
import org.junit.Test

import scala.collection.mutable

class NettyTest {

  @Test
  def test(): Unit = {
    val netEnv1 = new RedistributionNetworkEnvironment(new NioEventLoopGroup(), "localhost")
    val netEnv2 = new RedistributionNetworkEnvironment(new NioEventLoopGroup(), "localhost")
    val netEnv3 = new RedistributionNetworkEnvironment(new NioEventLoopGroup(), "localhost")
    val server = new RedistributeServer(new NettyServerHandler[String, Int](3,
      new AllNewStateArrivedListener[String, Int] {
        override def onAllNewStateArrived(statesByKey: mutable.HashMap[String, Int]): Unit = {
          statesByKey.foreach(println)
        }
      }), netEnv1)


    val serverAddr = server.bind()

    val c1 = new NettyRedistConnection[String, Int](serverAddr, netEnv1)
    val c2 = new NettyRedistConnection[String, Int](serverAddr, netEnv2)

    //    val c3 = new NettyRedistConnection(serverAddr, netEnv3)

    c1.sendState("a", 11)
    c2.sendState("b", 11)
    c1.sendState("c", 11)
    c1.sendState("d", 11)
    c1.sendEndOfRedistribution()
    c2.sendState("e", 11)
    //    c1.sendEndOfRedistribution()
    //    c1.sendEndOfRedistribution()
    c2.sendEndOfRedistribution()
    //    c3.sendEndOfRedistribution()

    //    Thread.sleep(5000)
    c1.close()
    c2.close()
    //    c2.stop()
    //    c3.stop()
    server.stop()
  }
}
