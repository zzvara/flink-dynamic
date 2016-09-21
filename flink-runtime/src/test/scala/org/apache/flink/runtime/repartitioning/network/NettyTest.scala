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
