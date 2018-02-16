package org.apache.flink.streaming.scala.examples.repartitioning

import hu.sztaki.drc.utilities.Distribution
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.repartitioning.RedistributeStateHandler
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.RollingSink

object RepartitioningCount {
  def main(args: Array[String]) {
    val sleepTimeInNanos = args(0).toInt
    val parallelism = args(1).toInt
    val exponent = args(2).toDouble
    val shift = args(3).toDouble
    val width = args(4).toInt
    RedistributeStateHandler.setPartitions(parallelism)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE)
    env.setParallelism(parallelism)

    env.addSource(new SourceFunction[String] {
      @transient lazy val distribution = Distribution.zeta(exponent, shift, width)

      var running = false

      override def run(sourceContext: SourceFunction.SourceContext[String]) = {
        running = true

        while (running) {
          sourceContext.collect(distribution.sample().toString)
          Thread.sleep(0, sleepTimeInNanos)
        }
      }

      override def cancel() = {
        running = false
      }
    })
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
        taskIndex.toString + "," + 1.toString + "," + System.currentTimeMillis().toString
      }
    })
    .setParallelism(parallelism)
    .map(x => x)
    .setParallelism(parallelism)
    .addSink(
      new RollingSink[String]("/development/dr-flink/repartitioning-count")
        .setBatchSize(1000 * 1000 * 400)
        .setPendingPrefix("p")
        .setInProgressPrefix("p")
    )

    env.execute("RepartitioningCount")
  }

}
