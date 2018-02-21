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

//import hu.sztaki.drc.{StrategyFactory, StreamingDecider}
//import hu.sztaki.drc.component.RepartitioningTrackerMaster
//import hu.sztaki.drc.partitioner.Partitioner

// TODO this was an attempt to use a streaming strategy
class FlinkStreamingStrategy(stageID: Int,
                             attemptID: Int,
                             numPartitions: Int,
                             resourceStateHandler: Option[() => Int]) {
//    extends StreamingDecider(stageID, attemptID, numPartitions, resourceStateHandler) {
//
//    override def getTrackerMaster: RepartitioningTrackerMaster[_, _, _, _, _] =
//      FlinkRepartitioningTrackerMaster.getInstance()
//
//    // TODO is synchronised needed?
//    protected override def resetPartitioners(newPartitioner: Partitioner): Unit = synchronized {
//      super.resetPartitioners(newPartitioner)
//      FlinkRepartitioningTrackerMaster.getInstance()
//        .setNewPartitioner(stageID, newPartitioner, currentVersion)
//    }
//  }
//
//  object FlinkDeciderStrategy {
//
//    implicit object FlinkDeciderStrategyFactory extends StrategyFactory[StreamingDecider] {
//      override def apply(stageID: Int, attemptID: Int, numPartitions: Int,
//                         resourceStateHandler: Option[() => Int]): StreamingDecider = {
//        new FlinkDeciderStrategy(stageID, attemptID, numPartitions, resourceStateHandler)
//      }
//    }
//  }

}
