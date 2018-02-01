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

import hu.sztaki.drc.{Sampler, ScannerFactory, Throughput}

class FlinkThroughput(totalSlots: Int,
                      histogramDrop: (Int, Long, Int, Sampler) => Unit)
  extends Throughput[FlinkTaskContext, FlinkTaskMetrics](totalSlots, histogramDrop) {

  override def whenStarted(): Unit = {
    super.whenStarted()
    // TODO add task completion listener to task context
  }

}

object FlinkThroughput {

  implicit object FlinkScannerFactory extends ScannerFactory[FlinkThroughput] {
    override def apply(totalSlots: Int,
                       histogramDrop: (Int, Long, Int, Sampler) => Unit)
    : FlinkThroughput =
      new FlinkThroughput(totalSlots, histogramDrop)
  }
}
