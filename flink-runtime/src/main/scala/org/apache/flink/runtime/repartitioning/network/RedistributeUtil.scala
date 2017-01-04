package org.apache.flink.runtime.repartitioning.network

import java.io.IOException
import java.net.SocketAddress
import java.util

import io.netty.handler.codec.serialization.{ClassLoaderClassResolver, ClassResolver, ObjectDecoder, ObjectEncoder}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}
import io.netty.util.CharsetUtil
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.commons.lang3.SerializationUtils

import scala.collection.mutable


trait StateAccessor[K, V] {
//  def isStateful: Boolean
  def getState: java.util.Map[K, V]
  def setState(state: java.util.Map[K, V]): Unit
}

class Success(val str: String) extends java.lang.Exception(str)

case class RedistributorAddress(value: SocketAddress)

case class RegisterTaskRedistributor(subtaskId: Int, numOfSubtasks: Int, addr: RedistributorAddress)

case class TaskRedistributorAddresses(addrs: Array[RedistributorAddress])

sealed trait RedistMsg
case class EndOfRedistributionMsg() extends RedistMsg
case class RedistributeStateMsg[K, V](key: K, value: V) extends RedistMsg

class RedistributionNetworkEnvironment(val eventLoopGroup: EventLoopGroup, val hostName: String)

trait BlockReleaserListener {
  @throws(classOf[IOException])
  def canReleaseBlock(): Unit
}

trait RedistributorConnection[K, V] {
  def sendState(k: K, v: V): Unit
  def sendEndOfRedistribution(): Unit
  def close(): Unit
}

class NettyObjSerializer {
  val encoder = new ObjectEncoder()
  val decoder = new ObjectDecoder(new ClassResolver() {
    override def resolve(className: String): Class[_] = {
      ClassLoader.getSystemClassLoader.loadClass(className)
    }
  })
}


class NettyRedistMsgSerializer {

  private val endOfRedistMark: Byte = 10
  private val redistStateMark: Byte = 11

  val encoder: MessageToByteEncoder[RedistMsg] = new MessageToByteEncoder[RedistMsg]() {
    override def encode(ctx: ChannelHandlerContext, msg: RedistMsg, out: ByteBuf): Unit = {
      msg match {
        case _: EndOfRedistributionMsg =>
          out.writeByte(endOfRedistMark)
        case msg@RedistributeStateMsg(k, v) =>
          out.writeByte(redistStateMark)
          val bytes = SerializationUtils.serialize(msg)
          out.writeInt(bytes.length)
          out.writeBytes(bytes)
      }
    }
  }

  val decoder: ByteToMessageDecoder = new ByteToMessageDecoder {
    override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
      // todo
    }
  }
}

trait AllNewStateArrivedListener[K, V] {
  def onAllNewStateArrived(statesByKey: mutable.HashMap[K, V]): Unit
}

class RedistStateBuilder[K, V](val numOfSubtasks: Int, private val listener: AllNewStateArrivedListener[K, V]) {
  private val map = new mutable.HashMap[K,V]()

  private var redistEndCnt = 0

  def onMsg(msg: RedistMsg): Unit = msg match {
    case EndOfRedistributionMsg() =>
      redistEndCnt += 1
      if (redistEndCnt >= numOfSubtasks - 1) {
        allNewStateArrived()
      }
    case RedistributeStateMsg(k, v) =>
      map.put(k.asInstanceOf[K], v.asInstanceOf[V])
  }

  private def allNewStateArrived(): Unit = {
    redistEndCnt = 0
    listener.onAllNewStateArrived(map)
    map.clear()
  }
}

@Sharable
class NettyServerHandler[K, V](val numOfSubtasks: Int,
                               private val listener: AllNewStateArrivedListener[K, V])
  extends SimpleChannelInboundHandler[Any] {

  private val stateBuilder = new RedistStateBuilder[K, V](numOfSubtasks, listener)

  override def channelRead0(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case x: RedistMsg =>
        stateBuilder.onMsg(x)
      case _ =>
        throw new RuntimeException(s"Illegal message: $msg")
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

}

class NettyClientHandler extends SimpleChannelInboundHandler[ByteBuf] {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
//    ctx.writeAndFlush(Unpooled.copiedBuffer("YEEEEY", CharsetUtil.UTF_8))
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
    //    println(s"Client received: ${msg.toString(CharsetUtil.UTF_8)}")
    msg.release()
  }
}

class NettyRedistConnection[K, V](val addr: RedistributorAddress, val netEnv: RedistributionNetworkEnvironment)
  extends RedistributorConnection[K, V] {

  val client = new RedistributeClient(addr, netEnv.eventLoopGroup, new NettyClientHandler)
  val ch = client.start().channel()

  override def sendState(k: K, v: V): Unit = {
    ch.write(RedistributeStateMsg(k, v))
  }

  override def sendEndOfRedistribution(): Unit = {
    ch.writeAndFlush(EndOfRedistributionMsg())
  }

  override def close(): Unit = {
    client.stop()
  }
}

class MockRedistributorConnection[K, V] extends RedistributorConnection[K, V] {
  private val re = new RuntimeException("Task should not redistribute state to itself.")

  override def sendState(k: K, v: V): Unit = throw re

  override def sendEndOfRedistribution(): Unit = {}

  override def close(): Unit = {}
}

object RedistributeUtil {
}
