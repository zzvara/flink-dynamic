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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import _root_.akka.actor._
import hu.sztaki.drc.component.RepartitioningTrackerMaster
import hu.sztaki.drc.messages.ShuffleWriteStatus
import hu.sztaki.drc.partitioner.Partitioner
import hu.sztaki.drc.Mode
import hu.sztaki.drc.{DeciderStrategy, ScannerFactory, StrategyFactory, Throughput}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint
import org.apache.flink.runtime.repartitioning.network.{RedistributorAddress, RegisterTaskRedistributor, TaskRedistributorAddresses}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

class FlinkRepartitioningTrackerMaster(masterRef: ActorRef)
                                      (implicit ev1: ScannerFactory[FlinkThroughput],
                                       ev2: StrategyFactory[DeciderStrategy])
  extends RepartitioningTrackerMaster[
    FlinkMessageable, FlinkCallContext, FlinkTaskContext, FlinkTaskMetrics, Any]()(ev1, ev2) {

  var mockNumOfTotalSlots: Option[(Int, Int)] = None
  var vertexParallelisms: Option[Map[Int, Int]] = None
  var redistributionResults = ArrayBuffer.empty[RedistributionResult]

  // TODO is volatile enough for syncing?
  @volatile
  var nextPartitioner: Option[(Int,Partitioner,Int)] = None

  def setVertexParallelisms(parallelismByVertex: Map[Int, Int]) = {
    vertexParallelisms = Some(parallelismByVertex)
  }

  master = new FlinkMessageable(masterRef)



  // ------------- redistribute logic ---------------

  // todo collect addresses, then broadcast them
  // todo get total num of partitions
  private var numPartitions: Int = -1
  private var redistributorAddresses: Array[RedistributorAddress] = _
  private var numOfRegisteredAddrs = 0

  def computeAndLogMigration(): Unit = {
    println {
      "MIGRATION: " +
        (redistributionResults.map(_.recordsMigrated.toDouble).sum /
          redistributionResults.map(_.recordsTotal.toDouble).sum)
    }
  }

  def registerRedistributor: PartialFunction[Any, Unit] = {
    case r : RedistributionResult =>
      redistributionResults += r
      computeAndLogMigration()
    case RegisterTaskRedistributor(subtaskId, numOfSubtasks, addr) =>
      if (numPartitions < 0) {
        numPartitions = numOfSubtasks
        redistributorAddresses = new Array[RedistributorAddress](numPartitions)
      }

      redistributorAddresses(subtaskId) = addr

      numOfRegisteredAddrs += 1

      logInfo(s"Registering redistributor address $addr for subtask $subtaskId " +
        s"($numOfRegisteredAddrs out of $numPartitions done)")

      if (numOfRegisteredAddrs >= numPartitions) {
        broadcastRedistributorAddresses()
      }
  }

  def broadcastRedistributorAddresses(): Unit = {
    logDebug(s"Broadcasting redistributor addresses.")
    workers.values.foreach(
      _.reference.send(TaskRedistributorAddresses(redistributorAddresses)))
  }

  // ------------------------------------------------

  override def getTotalSlots: Int = mockNumOfTotalSlots.map(_._2)
    .getOrElse {
      logError("Number of total slots have not been set, passing the default.")
      throw new RuntimeException("Number of total slots have not been set, passing the default.")
      super.getTotalSlots
    }

  def setNewPartitioner(stageID: Int, newPartitioner: Partitioner, currentVersion: Int): Unit = {
    nextPartitioner = Some((stageID, newPartitioner, currentVersion))
    // TODO optimization: trigger checkpoint here
  }

  def getNewPartitioner: Option[(Int, Partitioner, Int)] = {
    nextPartitioner
  }

  override def initializeLocalWorker(): Unit = {
    // TODO init local worker
  }

  override def whenStageSubmitted(jobID: Int,
                                  stageID: Int,
                                  attemptID: Int,
                                  repartitioningMode: Mode.Value): Unit =
    super.whenStageSubmitted(jobID, stageID, attemptID, repartitioningMode)

  override def componentReceiveAndReply(context: FlinkCallContext): PartialFunction[Any, Unit] = {
    val handleShuffleWriteStatus: PartialFunction[Any, Unit] = {
      case sws @ ShuffleWriteStatus(stageID, _, _, _) => {
        if (mockNumOfTotalSlots.isEmpty) {
          logInfo(s"Received histogram from vertex $stageID, " +
            s"using its parallelism (${vertexParallelisms.get(stageID)}) to " +
            s"mock number of total slots.")
          mockNumOfTotalSlots = Some(stageID, vertexParallelisms.get(stageID))
          super.componentReceiveAndReply(context)(sws)
        } else if (mockNumOfTotalSlots.get._1 != stageID) {
          logError(s"Received histogram from vertex $stageID," +
            s"while another vertex is being monitor." +
            s"Currently monitoring only one vertex is supported in Flink" +
            s"dynamic repartitioning.")
          /*
          throw new RuntimeException(
            s"Received histograms from multiple vertices." + s"This is not supported in Flink.")
            */
        } else {
          super.componentReceiveAndReply(context)(sws)
        }
      }
    }

    handleShuffleWriteStatus
      .orElse(super.componentReceiveAndReply(context))
      .orElse(registerRedistributor)
  }

  override def scannerFactory(): ScannerFactory[
    Throughput[FlinkTaskContext, FlinkTaskMetrics]] =
      implicitly[ScannerFactory[FlinkThroughput]]
}

object FlinkRepartitioningTrackerMaster {

  private var instance: Option[FlinkRepartitioningTrackerMaster] = None

  def getInstance(): FlinkRepartitioningTrackerMaster = instance match {
    case None => {
      // TODO how to use log here?
      println("ERROR: RT Master not yet set while trying to access singleton.")
      throw new RuntimeException("RT Master not yet set while trying to access singleton.")
    }
    case Some(rtm) => rtm
  }

  def setInstance(rtm: FlinkRepartitioningTrackerMaster) = {
    instance match {
      case None => instance = Some(rtm)
      case Some(_) => {
        // TODO how to use log here?
        println("ERROR: RT Master already set.")
        throw new RuntimeException("RT Master already set at singleton.")
      }
    }
  }

  case class TriggerCheckpointWithPartitioner(partitionerVersion: Int, trigger: TriggerCheckpoint)

  def checkpointTriggerCreator(): (JobID, ExecutionAttemptID, Long, Long) =>
    java.io.Serializable = {
    if (getInstance().nextPartitioner.isDefined) {
      val partitionerVersion = getInstance().nextPartitioner.get._3
      getInstance().nextPartitioner = None

      (job: JobID, id: ExecutionAttemptID, checkpointID: Long, timestamp: Long) =>
        TriggerCheckpointWithPartitioner(partitionerVersion,
          new TriggerCheckpoint(job, id, checkpointID, timestamp))
    } else {
      (job: JobID, id: ExecutionAttemptID, checkpointID: Long, timestamp: Long) =>
        new TriggerCheckpoint(job, id, checkpointID, timestamp)
    }
  }

}

case class RedistributionResult(subtaskId: Int, recordsTotal: Long, recordsMigrated: Long, totalTimeMs: Long)
