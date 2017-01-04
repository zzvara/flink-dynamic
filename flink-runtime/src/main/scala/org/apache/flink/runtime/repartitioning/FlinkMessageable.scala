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

import java.util.concurrent.TimeUnit

import _root_.akka.actor._
import _root_.akka.pattern.ask
import _root_.akka.util.Timeout
import hu.sztaki.drc.utilities.Messageable

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

class FlinkMessageable(actorRef: ActorRef) extends Messageable with Serializable {

  val maxAttempts = 10
  val waitTimeInMillis = 5000

  override def send(message: Any): Unit = {
    actorRef ! message
  }

  override def askWithRetry[T: ClassTag](message: Any): T = {
    var attempts = 0

    // TODO clean this heck
    while (attempts < maxAttempts) {
      attempts += 1

      val future = actorRef.ask(message)(Timeout.longToTimeout(waitTimeInMillis))
        .asInstanceOf[Future[T]]
      val result = Await.result[T](future, Duration(waitTimeInMillis, TimeUnit.MILLISECONDS))

      return result
    }

    throw new RuntimeException("Failed to deliver message.")
  }
}
