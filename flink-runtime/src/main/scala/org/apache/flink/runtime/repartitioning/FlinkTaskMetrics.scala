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

import hu.sztaki.drc.{Metrics, Sampler}
import hu.sztaki.drc.partitioner.RepartitioningInfo

class FlinkTaskMetrics extends Metrics[FlinkTaskMetrics] {

  override var repartitioningInfo: Option[RepartitioningInfo[FlinkTaskMetrics]] = None

  // fixme place here data characteristics accum
  private val dcAcc = new Sampler {
  }

  // todo avoid sync?
  def add(v: (Any, Double)): Unit =
    dcAcc.add(v)

  // todo avoid sync?
  override def writeCharacteristics: Sampler =
    dcAcc
}