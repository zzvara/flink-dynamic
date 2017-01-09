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

package org.apache.flink.runtime.repartitioning

import java.util.concurrent.atomic.AtomicBoolean

import _root_.akka.actor._
import grizzled.slf4j.Logger
import hu.sztaki.drc.Context
import hu.sztaki.drc.partitioner.Partitioner
import io.netty.channel.ChannelHandler
import org.apache.flink.runtime.io.network.api.{CheckpointBarrier, CheckpointBarrierWithRepartitioner}
import org.apache.flink.runtime.repartitioning.network._

import scala.collection.mutable

// TODO force exactly once barrier semantics!!

class FlinkTaskContext(val taskName: String,
                       private val partition: Int,
                       private val idAttempt: Int,
                       private val idStage: Int,
                       private val attemptNum: Int,
                       val numOfSubtasks: Int,
                       val hasKeyedState: Boolean,
                       private val masterRef: ActorRef,
                       private val redistNetEnv: RedistributionNetworkEnvironment)
  extends Context[FlinkTaskMetrics] {

  val redistributeStateHandler =
    if (hasKeyedState) {
      Some(new RedistributeStateHandler(
        masterRef, partition, numOfSubtasks, hasKeyedState, redistNetEnv))
    } else {
      None
    }

  val log = Logger(getClass)

  // -------------------------------------------

  private val taskMetrics = new FlinkTaskMetrics()

  var partitioningByHash: Boolean = false

  var barrierHasArrived: AtomicBoolean = new AtomicBoolean(false)

  def setPartitioningByHash() = {
    partitioningByHash = true
  }

  private var partitionersByVersion: mutable.HashMap[Int, Partitioner] =
    new mutable.HashMap[Int, Partitioner]()
  private var partitionerListener: PartitionerChangeListener = null
  private var bufferingStateListener: BufferingStateListener = null

  def registerBufferingStateListener(listener: BufferingStateListener): Unit = {
    bufferingStateListener = listener
  }

  // todo call this when partitioner must change
  def registerPartitionerChangeListener(listener: PartitionerChangeListener): Unit = {
    partitionerListener = listener
  }

  def onBarrierArrival(barrier: CheckpointBarrierWithRepartitioner): Unit = {
    val ver = barrier.getPartitionerVersion
    partitionersByVersion.get(ver)
      .map { partitioner =>
        log.info(s"New partitioner v$ver is already here on barrier arrival, we can swap it.")
        partitionerListener.onPartitionerChange(partitioner)
      }
      .getOrElse {
        log.info(s"New partitioner v$ver has not arrived yet on barrier arrival," +
          s"we should start buffering.")
        bufferingStateListener.onStartBuffering()
        barrierHasArrived.set(true)
      }
  }

  def onNewPartitionerArrive(partitioner: Partitioner, version: Int): Unit = {

    if (barrierHasArrived.get()) {
      log.info(s"Barrier has already arrived on partitioner v$version arrival @$partition," +
        s" we can swap the partitioner and send the buffer through.")
      partitionerListener.onPartitionerChange(partitioner)
      bufferingStateListener.onFinishBuffering()
      barrierHasArrived.set(false)
    } else {
      log.info(s"Barrier has not arrived on partitioner v$version arrival @$partition," +
        s" storing the partitioner and waiting for barrier arrival.")
      partitionersByVersion += version -> partitioner
    }
  }

  def close(): Unit = {
    redistributeStateHandler.foreach(_.shutdownNetwork())
  }

  override def metrics(): FlinkTaskMetrics = {
    taskMetrics
  }

  override def partitionID(): Int = {
    partition
  }

  override def attemptID(): Int = idAttempt

  override def stageID(): Int = idStage

  override def attemptNumber(): Int = attemptNum


}
