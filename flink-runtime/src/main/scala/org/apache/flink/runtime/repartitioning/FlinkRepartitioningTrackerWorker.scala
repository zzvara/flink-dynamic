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

import java.net.InetSocketAddress

import _root_.akka.actor._
import _root_.akka.pattern.ask
import hu.sztaki.drc.component.RepartitioningTrackerWorker
import hu.sztaki.drc.messages.RepartitioningStrategy
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.flink.runtime.instance.ActorGateway
import org.apache.flink.runtime.repartitioning.network.{RedistributionNetworkEnvironment, RegisterTaskRedistributor, TaskRedistributorAddresses}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class FlinkRepartitioningTrackerWorker(val selfRef: ActorRef,
                                       val masterRef: ActorRef,
                                       val execID: String,
                                       val serverAddr: String)
extends RepartitioningTrackerWorker[FlinkMessageable, FlinkMessageable,
  FlinkTaskContext, FlinkTaskMetrics, Any](execID) {

  private val taskContextsByStageId: mutable.HashMap[Int, mutable.MutableList[FlinkTaskContext]] =
    new mutable.HashMap[Int, mutable.MutableList[FlinkTaskContext]]()

  val redistNetEnv = new RedistributionNetworkEnvironment(new NioEventLoopGroup(), serverAddr)

  // todo use something else for buffer?
  // cache task arrivals by stage ID if no strategy has arrived yet
  protected val taskArrivalCache =
    mutable.HashMap[Int, mutable.ListBuffer[(Long, FlinkTaskContext)]]()

  val selfRefMessageable = new FlinkMessageable(selfRef)

  master = new FlinkMessageable(masterRef)

  override def selfReference: FlinkMessageable = selfRefMessageable

  // -------------- redistribution --------------------

  private def handleRedistMessages: PartialFunction[Any, Unit] = {
    case TaskRedistributorAddresses(addrs) =>
      for {
        tcsByStage <- taskContextsByStageId.values
        tc <- tcsByStage
        if tc.hasKeyedState
      } yield {
        tc.redistributeStateHandler.foreach(_.registerTaskRedistributorAddresses(addrs))
      }
  }
  // --------------------------------------------------

  private def privateTaskArrival(taskID: Long,
                                 stageID: Int,
                                 taskContext: FlinkTaskContext): Unit = {
    super.taskArrival(taskID, stageID, taskContext)
    val tasksAtStage = taskContextsByStageId
      .getOrElse(stageID, new mutable.MutableList[FlinkTaskContext]())
    taskContextsByStageId.put(stageID, tasksAtStage.+:(taskContext))
    logDebug(s"Updated task list for stage $stageID: $tasksAtStage")
  }

  // todo why is sync needed? because of mutable hashmap?
  override def taskArrival(taskID: Long, stageID: Int, taskContext: FlinkTaskContext): Unit =
  this.synchronized {
    stageData.get(stageID) match {
      case Some(sd) =>
        privateTaskArrival(taskID, stageID, taskContext)
      case None =>
        val cachedTasksAtStage = taskArrivalCache
          .getOrElse(stageID, new mutable.ListBuffer[(Long, FlinkTaskContext)])
        cachedTasksAtStage += ((taskID, taskContext))
        logInfo(s"Non-registered stage for task $taskID of stage $stageID," +
          s" cached task arrival until stage gets registered.")
    }
  }

  // todo why is sync needed?
  override def componentReceive: PartialFunction[Any, Unit] = this.synchronized {
    val repartStrategyHandler: PartialFunction[Any, Unit] = {
      case x@RepartitioningStrategy(stageID, repartitioner, version) =>
        super.componentReceive(x)

        // handling mapper side
        taskContextsByStageId.get(stageID).toList.flatten.foreach {
          _.onNewPartitionerArrive(repartitioner, version)
        }

        // handling reducer side
        for {
          tcs <- taskContextsByStageId.values
          tc <- tcs
          if tc.hasKeyedState
        } yield {
          tc.redistributeStateHandler.foreach(_.onPartitionerArrival(repartitioner))
        }
    }

    repartStrategyHandler
      .orElse(handleRedistMessages)
      .orElse(super.componentReceive
        .andThen((Unit) => registerCachedTasks()))
  }

  def registerCachedTasks(): Unit = {
    for {
      (stageID, taskArrivals) <- taskArrivalCache
      if stageData.contains(stageID)
      (taskID, taskContext) <- taskArrivals
    } yield {
      privateTaskArrival(taskID, stageID, taskContext)
      logInfo(s"Registered cached task $taskID at arriving stage $stageID")
    }

    // removing registered tasks from cache
    for {
      stageID <- stageData.keySet
    } yield {
      taskArrivalCache.remove(stageID)
    }
  }

  def close(): Unit = {
    for {
      tcsAtStage <- taskContextsByStageId.values
      tc <- tcsAtStage
    } yield {
      tc.close()
    }
  }

}

