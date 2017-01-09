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

import java.io.IOException

import hu.sztaki.drc.partitioner.Partitioner
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.runtime.io.network.api.CheckpointBarrier

import scala.collection.mutable.ArrayBuffer

object RepartitioningUtil {

}

trait BufferingStateListener extends Serializable {
  def onStartBuffering(): Unit
  def onFinishBuffering(): Unit
}

trait RecordEmitter[T] extends Serializable {

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def emit(rec: T): Unit

}

class MaybeBufferingEmitter[T](val defaultEmitter: RecordEmitter[T])
  extends RecordEmitter[T] with BufferingStateListener {
  private var emitter: RecordEmitter[T] = defaultEmitter
  private val bufferingEmitter: BufferingEmitter[T] = new BufferingEmitter[T](defaultEmitter)

  override def emit(rec: T): Unit = { emitter.emit(rec) }

  def onStartBuffering(): Unit = { emitter = bufferingEmitter }

  def onFinishBuffering(): Unit = { emitter = defaultEmitter }

}

class BufferingEmitter[T](val defaultEmitter: RecordEmitter[T]) extends RecordEmitter[T] {

  // todo what datastructure to use?
  private var buffer: ArrayBuffer[T] = new ArrayBuffer[T](10)

  override def emit(rec: T): Unit = {
    buffer += rec
  }

  def emitAndClear(): Unit = {
    for (rec <- buffer) {
      defaultEmitter.emit(rec)
    }
    buffer.clear()
  }
}

trait PartitionerArrivalListener extends Serializable {
  def onNewPartitionerArrival(partitioner: Partitioner, version: Int)
}

trait PartitionerChangeListener extends Serializable {
  def onPartitionerChange(partitioner: Partitioner)
}



