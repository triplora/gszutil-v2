/*
 * Copyright 2019 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gszutil.io

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, Broadcast, RoundRobinRoutingLogic, Router}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.orc.Protocol

import scala.collection.mutable

class V2ReceiveActor(args: V2ActorArgs) extends Actor with Logging {
  private val timer = new WriteTimer
  private val writers = mutable.Set[ActorRef]()

  override def preStart(): Unit = {
    val routees = (1 to args.nWriters).map{ i =>
      val r = context.actorOf(Props(classOf[V2WriteActor], args), s"w$i")
      writers.add(r)
      context.watch(r)
      ActorRefRoutee(r)
    }.toArray.toIndexedSeq
    val router = Router(RoundRobinRoutingLogic(), routees)
    val receiver = new V2ReceiveCallable(args.socket,
      args.blkSize, args.compress, args.pool, router, context)
    context.become(active(router, receiver))
    self ! "start"
  }

  override def receive: Receive = {
    case _ =>
  }

  def active(router: Router, receiver: V2ReceiveCallable): Receive = {
    case s: String if s == "start" =>
      timer.start()
      val result = receiver.call()
      timer.end()
      if (result.isDefined)
        self ! result.get
      else
        self ! "failed"

    case result: ReceiveResult =>
      timer.close(logger, "Finished Receiving", result.bytesIn, result.bytesOut)
      context.become(stopping(result.bytesIn, result.bytesOut, success = true))
      router.route(Broadcast("finished"), self)

    case s: String if s == "failed" =>
      timer.close(logger, "Receive failed", -1,-1)
      context.become(stopping(-1, -1, success = false))
      router.route(Broadcast("finished"), self)

    case Terminated(ref) =>
      writers.remove(ref)

    case _ =>
  }

  def stopping(bytesIn: Long, bytesOut: Long, success: Boolean): Receive = {
    case Terminated(ref) =>
      writers.remove(ref)
      if (writers.isEmpty)
        if (success)
          args.notifyActor ! Protocol.UploadComplete(bytesIn, bytesOut)
        else
          args.notifyActor ! Protocol.Failed
    case _ =>
  }
}
