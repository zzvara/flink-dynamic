package org.apache.flink.streaming.scala.examples

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.plugable.SerializationDelegate
import org.apache.flink.streaming.runtime.partitioner.HashPartitioner
import org.apache.flink.streaming.runtime.streamrecord.{StreamRecord, StreamRecordSerializer}
import org.junit.Test

import scala.util.hashing.MurmurHash3

class RepartitioningCase {
  @Test
  def test(): Unit = {
    val hashPartitioner = new HashPartitioner[String](new KeySelector[String, String] {
      override def getKey(in: String) = in
    })

    val recordSerializer = new StreamRecordSerializer[String](new StringSerializer)
    val data = (1 to 1000000).map {
      i => MurmurHash3.stringHash(i.toString, -543264326).toString
    }.map {
      k =>
        val serializationDelegate = new SerializationDelegate[StreamRecord[String]](recordSerializer)
        serializationDelegate.setInstance(new StreamRecord[String](k, System.currentTimeMillis()))
        serializationDelegate
    }

    (1 to 10).foreach {
      _ =>
        val t = System.currentTimeMillis()

        data.foreach(d => hashPartitioner.selectChannels(d, 50))

        println(System.currentTimeMillis() - t)
    }
  }
}
