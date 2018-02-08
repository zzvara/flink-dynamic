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

import akka.actor.ActorRef
import grizzled.slf4j.Logger
import hu.sztaki.drc.partitioner.Partitioner
import org.apache.flink.runtime.repartitioning.network._

import scala.collection.mutable
import collection.JavaConverters._

class RedistributeStateHandler(
  val masterRef: ActorRef,
  val partition: Int,
  val numOfSubtasks: Int,
  val hasKeyedState: Boolean,
  val redistNetEnv: RedistributionNetworkEnvironment) {

  private val log = Logger(getClass)

  var blockReleaserListener: Option[BlockReleaserListener] = None

  private var redistServer: Option[RedistributeServer] = None

  private var barrierArrived: Boolean = false
  private var partitionerArrived: Boolean = false
  private var allNewStateArrived: Boolean = false
  private var redistributedAll: Boolean = false

  private var localState: Option[mutable.HashMap[Any, Any]] = None
  private var allNewState: Option[mutable.HashMap[Any, Any]] = None
  private var partitioner: Option[Partitioner] = None
  
  var stateAccessor: StateAccessor[Any, Any] = _

  def setStateAccessor(accessor: StateAccessor[Any, Any]) {
    if (stateAccessor == null) {
      stateAccessor = accessor
    } else if (stateAccessor != accessor) {
      throw new RuntimeException("Cannot set stateHolder twice")
    }
  }

  // todo use real handler
  if (hasKeyedState) {
    redistServer = Some(new RedistributeServer(new NettyServerHandler[Any, Any](10,
      new AllNewStateArrivedListener[Any, Any] {
        override def onAllNewStateArrived(statesByKey: mutable.HashMap[Any, Any]): Unit = {
          log.info(s"All new state has arrived at subtask $partition: $statesByKey")
          allNewState = Some(statesByKey)
          onAllStateArrived()
        }
      }),
      redistNetEnv))
  }

  val serverAddr = redistServer.map(_.bind())
  sendRedistributorRegistration()

  private def sendRedistributorRegistration(): Unit = {
    serverAddr.foreach(addr =>
      masterRef ! RegisterTaskRedistributor(partition, numOfSubtasks, addr)
    )
  }

  val redistConnections = new Array[RedistributorConnection[Any, Any]](numOfSubtasks)

  def sendEndOfRedistToAll(): Unit = {
    for (c <- redistConnections) {
      c.sendEndOfRedistribution()
    }
  }

  def registerTaskRedistributorAddresses(addrs: Array[RedistributorAddress]): Unit = {
    for {
      subtaskIdx <- Seq.range(0, numOfSubtasks)
      if subtaskIdx != partition
    } yield {
      redistConnections(subtaskIdx) =
        new NettyRedistConnection(addrs(subtaskIdx), redistNetEnv)
    }

    redistConnections(partition) = new MockRedistributorConnection

    log.info(s"Received redistribution addresses at task $partition.")
  }

  def setBlockReleaserListener(blockReleaserListener: BlockReleaserListener) {
    this.blockReleaserListener = Some(blockReleaserListener)
  }

  def shutdownNetwork(): Unit = {
    redistServer.foreach(_.stop())
    redistConnections.foreach(_.close())
  }

  // logic

  private def redistributeState(): Unit = {
    log.info(s"Redistributing from subtask $partition")

    val p = partitioner.get
    val states = stateAccessor.getState.asScala
    val local = new mutable.HashMap[Any, Any]()

    for ((k, v) <- states) {
      val targetPartition = p.get(k)

      if (targetPartition == partition) {
        local += k -> v
      } else {
        redistConnections(targetPartition).sendState(k, v)
      }
    }

    sendEndOfRedistToAll()

    localState = Some(local)
    onRedistributedAll()
  }

  private def onRedistributedAll(): Unit = {
    log.info(s"Redistributed all from subtask $partition")

    redistributedAll = true
    if (allNewStateArrived) {
      goOnProcessing()
    }
  }

  private def onAllStateArrived(): Unit = {
    log.info(s"All new state has arrived at subtask $partition")

    allNewStateArrived = true
    if (redistributedAll) {
      goOnProcessing()
    }
  }

  def onBarrierArrival(): Unit = {
    log.info(s"All barrier has arrived at subtask $partition")

    barrierArrived = true
    if (partitionerArrived) {
      redistributeState()
    }
  }


  def onPartitionerArrival(p: Partitioner): Unit = {
    log.info(s"Partitioner has arrived at subtask $partition")

    partitionerArrived = true
    partitioner = Some(p)
    if (barrierArrived) {
      redistributeState()
    }
  }

  private def goOnProcessing(): Unit = {
    log.info(s"Going on with processing at subtask $partition")

    // swapping state at operator
    val newState = allNewState.get ++ localState.get
    stateAccessor.setState(newState.asJava)
    log.debug(s"State swapped to at subtask $partition: ${stateAccessor.getState}")

    barrierArrived = false
    redistributedAll = false
    partitionerArrived = false
    allNewStateArrived = false
    partitioner = None
    allNewState = None
    localState = None
    blockReleaserListener.get.canReleaseBlock()
  }
}
