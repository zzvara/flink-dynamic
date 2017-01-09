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

package org.apache.flink.streaming.api.scala.repartitioning

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

class RepartioningITCase2 {

  import RepartioningITCase2._

  @Test
  def test(): Unit = {
    val sleepTimeInMillis: Long = 5
    val totalNumberOfItems: Long = 1000

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.noRestart())

    env.setParallelism(4)

    env.addSource(new SourceFunction[String]() {

      var running = false

      override def run(ctx: SourceContext[String]): Unit = {

        val keys = Array("a", "b", "c", "d", "e", "f")
        val random = new Random()

        running = true

        var cnt = 0
        while (cnt < totalNumberOfItems && running) {
          val randomKey = keys(random.nextInt(keys.length))
          ctx.collect(randomKey)
          Thread.sleep(sleepTimeInMillis)

          cnt += 1
        }
      }

      override def cancel(): Unit = {
        running = false
      }

    }).map(x => x).setParallelism(4)
      .keyBy(x => x)
      .map(new RichMapFunction[String, (String, Int, Int)]() {

        @transient
        private var cnt: ValueState[Int] = _

        override def open(config: Configuration): Unit = {
          val descriptor =
            new ValueStateDescriptor[Int](
              "cnt", // the state name
              TypeInformation.of(new TypeHint[Int]() {}), // type information
              0) // default value of the state, if nothing was set

          cnt = getRuntimeContext.getState(descriptor)
        }

        override def map(value: String): (String, Int, Int) = {
          val currCnt = cnt.value() + 1
          cnt.update(currCnt)
          val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
          (value, currCnt, subtaskIdx)
        }
      }).setParallelism(4)
      .map(x => x).setParallelism(4)
      .addSink(new SinkFunction[(String, Int, Int)]() {
        val cntMap = new mutable.HashMap[String, Int]()
        val subtaskMap = new mutable.HashMap[String, Set[Int]]()

        override def invoke(value: (String, Int, Int)): Unit = {
          val (key, cnt, subtaskIdx) = value

          cntMap.put(key, cnt)

          subtaskMap.put(key, subtaskMap.getOrElse(key, Set.empty) + subtaskIdx)
          if (cntMap.values.sum == totalNumberOfItems) {
            if (subtaskMap.values.map(_.size).max > 1) {
              success()
            } else {
              fail(s"Expected at least 1 key to be repartitioned, but none were.")
            }
          }
        }
      }).setParallelism(1)

      val isSuccess =
        try {
          env.execute()
        } catch {
          case e: JobExecutionException =>
            if (e.getCause.isInstanceOf[TestSuccess]) {
              true
            } else {
              org.junit.Assert.fail(e.getCause.getMessage)
            }
          case e =>
            org.junit.Assert.fail(e.getMessage)
        }

    if (isSuccess != true) {
      org.junit.Assert.fail("Did not receive success exception.")
    }
  }

}

object RepartioningITCase2 {
  class TestFailure(val msg: String) extends Exception
  class TestSuccess extends Exception

  def fail(msg: String): Unit = throw new TestFailure(msg)
  def success(): Unit = throw new TestSuccess()

}
