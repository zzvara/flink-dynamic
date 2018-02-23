package org.apache.flink.streaming.scala.examples.repartitioning

import hu.sztaki.drc.utilities.Distribution
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.runtime.repartitioning.RedistributeStateHandler
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.RollingSink

import scala.util.hashing.MurmurHash3
import scala.util.{Random, Try}

object RepartitioningCount {
  def main(args: Array[String]) {
    val parallelism = args(0).toInt
    val sources = args(1).toInt
    val exponent = args(2).toDouble
    val shift = args(3).toDouble
    val width = args(4).toInt
    val complexity = args(5).toInt
    val sleepMillis = Try(args(6).toInt).getOrElse(-1)
    val sleepNanos = Try(args(7).toInt).getOrElse(-1)

    RedistributeStateHandler.setPartitions(parallelism)

    // GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR"))
    // val conf = GlobalConfiguration.getConfiguration
    // conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    // val env = new StreamExecutionEnvironment(new environment.LocalStreamEnvironment(conf))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE)
    env.setParallelism(parallelism)

    val seed = Random.nextInt(Int.MaxValue)

    env.addSource(new ParallelSourceFunction[String] {
      @transient lazy val distribution = Distribution.zeta(exponent, shift, width)

      var running = false

      override def run(sourceContext: SourceFunction.SourceContext[String]) = {
        running = true

        if (sleepNanos == -1) {
          while (running) {
            sourceContext.collect(
              MurmurHash3.stringHash(distribution.sample().toString, seed)
                .toString
            )
          }
        } else {
          while (running) {
            sourceContext.collect(
              MurmurHash3.stringHash(distribution.sample().toString, seed)
                .toString
            )
            Thread.sleep(sleepMillis, sleepNanos)
          }
        }
      }

      override def cancel() = {
        running = false
      }
    })
      .setParallelism(sources)
      .map(x => x)
      .setParallelism(parallelism)
      .keyBy(x => x)
      .map(new RichMapFunction[String, String] {
        @transient private var count: ValueState[Int] = _
        @transient private var taskIndex: Int = _

        override def open(parameters: Configuration) = {
          val descriptor = new ValueStateDescriptor[Int](
            "count",
            TypeInformation.of(new TypeHint[Int] {}),
            0
          )
          count = getRuntimeContext.getState(descriptor)
          taskIndex = getRuntimeContext.getIndexOfThisSubtask
        }

        override def map(value: String) = {
          count.update(count.value() + 1)
          if (complexity != -1) {
            Thread.sleep(complexity)
          }
          taskIndex.toString + "," + System.currentTimeMillis().toString
        }
      })
      .map(x => x)
      .addSink(
        new RollingSink[String]("/development/dr-flink/repartitioning-count/" + System.currentTimeMillis() + "/")
          .setBatchSize(1000 * 1000 * 100)
          .setPendingPrefix("p")
          .setInProgressPrefix("p")
      )
      .setParallelism(sources)

    env.execute("RepartitioningCount")
  }

}
