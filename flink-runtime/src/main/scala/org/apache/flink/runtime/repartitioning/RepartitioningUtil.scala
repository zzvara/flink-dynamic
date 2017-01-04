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



