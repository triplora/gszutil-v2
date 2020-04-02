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

package com.google.cloud.gszutil.orc

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, EscalatingSupervisorStrategy, Props, SupervisorStrategy,
  Terminated}
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{V2WriterArgs, V2WriteActor, ZRecordReaderT}
import com.google.cloud.gszutil.orc.Protocol.{PartComplete, PartFailed, UploadComplete}
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/** Responsible for reading from input Data Set and creating ORC Writers
  * Creates writer child actors at startup
  */
class DatasetReader(args: DatasetReaderArgs) extends Actor with Logging {
  private var startTime: Long = -1
  private var endTime: Long = -1
  private var lastSend: Long = -1
  private var lastRecv: Long = -1
  private var activeTime: Long = 0
  private var nSent = 0L
  private var totalBytesRead = 0L
  private var totalBytesWritten: Long = 0
  private var continue = true
  private val lrecl =
    args.in match {
      case x: ZRecordReaderT => x.lRecl
      case _ => args.schemaProvider.LRECL
    }

  override def preStart(): Unit = {
    startTime = System.currentTimeMillis
    for (_ <- 0 until args.nWorkers)
      newPart()
  }

  override def receive: Receive = {
    case bb: ByteBuffer =>
      lastRecv = System.currentTimeMillis
      bb.clear()
      var n = 0 // number of bytes read
      while (bb.remaining >= lrecl && continue) {
        n = args.in.read(bb)
        if (n < 0){
          logger.info(s"${args.in.getClass.getSimpleName} reached end of input")
          continue = false
        }
      }

      totalBytesRead += bb.position
      bb.flip()
      sender ! bb
      nSent += 1
      lastSend = System.currentTimeMillis
      activeTime += (lastSend - lastRecv)
      if (!continue) {
        val mbps = Util.fmbps(totalBytesRead,activeTime)
        logger.info(s"Finished reading $nSent chunks with $totalBytesRead bytes in $activeTime ms ($mbps mbps)")
        for (w <- writers) w ! Protocol.Close
        context.become(finished)
      }

    case Terminated(w) =>
      writers.remove(w)
      newPart()

    case msg: PartComplete =>
      totalBytesWritten += msg.bytesWritten

    case msg: PartFailed =>
      args.notifyActor ! msg

    case msg =>
      logger.error(s"Unhandled message: $msg")
  }

  def finished: Receive = {
    case msg: PartComplete =>

    case msg: PartFailed =>
      args.notifyActor ! msg

    case msg: Protocol.FinishedWriting =>
      totalBytesRead += msg.bytesIn
      totalBytesWritten += msg.bytesOut

    case Terminated(w) =>
      writers.remove(w)
      if (writers.isEmpty)
        args.notifyActor ! UploadComplete(totalBytesRead, totalBytesWritten)

    case _: ByteBuffer =>

    case msg =>
      logger.warn(s"Ignoring ${msg.getClass.getSimpleName} from $sender")
  }

  private var partId = 0
  private val writers = mutable.Set.empty[ActorRef]

  private def newPart(): Unit = {
    val partName = f"$partId%05d"
    val basePath = new Path(s"gs://${args.uri.getAuthority}/${args.uri.getPath.stripPrefix("/")}")
    val actorArgs = V2WriterArgs(
      schemaProvider = args.schemaProvider,
      partitionBytes = args.maxBytes,
      basePath = basePath,
      gcs = args.gcs,
      pool = args.pool,
      maxErrorPct = args.maxErrorPct)
    val w = context.actorOf(Props(classOf[V2WriteActor], actorArgs), partName)
    w ! Protocol.Start
    context.watch(w)
    writers.add(w)
    partId += 1
  }

  override def postStop(): Unit = {
    endTime = System.currentTimeMillis
    val totalTime = endTime - startTime
    val mbps = Util.fmbps(totalBytesWritten, totalTime)
    val wait = totalTime - activeTime
    logger.info(s"DatasetReader stopping after $totalBytesRead bytes read $totalBytesWritten bytes written; $nSent chunks; $totalTime ms; $mbps mbps; active $activeTime ms; wait $wait ms")
  }

  override def supervisorStrategy: SupervisorStrategy = new EscalatingSupervisorStrategy().create()
}
