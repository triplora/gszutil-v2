/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import akka.actor.{Actor, ActorRef, EscalatingSupervisorStrategy, Props, SupervisorStrategy, Terminated}
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
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
  private var totalBytes = 0L
  private var continue = true
  private var debugLogCount = 0
  import args._

  override def preStart(): Unit = {
    startTime = System.currentTimeMillis
    for (_ <- 0 until nWorkers)
      newPart()
  }

  override def receive: Receive = {
    case bb: ByteBuffer =>
      if (logger.isDebugEnabled && debugLogCount < 1) {
        logger.debug("received ByteBuffer")
        debugLogCount += 1
      }
      lastRecv = System.currentTimeMillis
      bb.clear()
      var k = 0 // number of read attempts returning 0 bytes
      var n = 0 // number of bytes read
      while (bb.hasRemaining && k < 5 && continue) {
        n = in.read(bb)
        if (n == 0) k += 1
        if (n < 0){
          logger.info(s"${in.getClass.getSimpleName} reached end of input")
          continue = false
        }
      }
      if (k >= 5 && debugLogCount < 2) {
        logger.debug(s"0 bytes read from $k read attempts")
        debugLogCount += 1
      }

      totalBytes += bb.position
      bb.flip()
      sender ! bb
      nSent += 1
      lastSend = System.currentTimeMillis
      activeTime += (lastSend - lastRecv)
      if (!continue) {
        val mbps = Util.fmbps(totalBytes,activeTime)
        logger.info(s"Finished reading $nSent chunks with $totalBytes bytes in $activeTime ms ($mbps mbps)")
        context.become(finished)
      }

    case Terminated(w) =>
      writers.remove(w)
      newPart()

    case _ =>
  }

  def finished: Receive = {
    case _: ByteBuffer =>
      context.stop(sender)

    case Terminated(w) =>
      writers.remove(w)
      if (writers.isEmpty)
        context.stop(self)

    case _ =>
  }

  private var partId = 0
  private val writers = mutable.Set.empty[ActorRef]

  private def newPart(): Unit = {
    val partName = f"$partId%05d"
    val path = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/") + s"/part-$partName.orc"}")
    val args = ORCFileWriterArgs(copyBook, maxBytes, batchSize, path, gcs, compress, pool)
    val w = context.actorOf(Props(classOf[ORCFileWriter], args), s"OrcWriter-$partName")
    context.watch(w)
    writers.add(w)
    partId += 1
  }

  override def postStop(): Unit = {
    endTime = System.currentTimeMillis
    val totalTime = endTime - startTime
    val mbps = Util.fmbps(totalBytes, totalTime)
    val wait = totalTime - activeTime
    logger.info(s"Finished writing $totalBytes bytes; $nSent chunks; $totalTime ms; $mbps mbps; active $activeTime ms; wait $wait ms")
    context.system.terminate()
  }

  override def supervisorStrategy: SupervisorStrategy = new EscalatingSupervisorStrategy().create()
}
