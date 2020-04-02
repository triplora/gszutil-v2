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

import java.nio.ByteBuffer

import akka.actor.Actor
import com.google.cloud.gszutil.PackedDecimal
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.orc.Protocol

final class V2WriteActor(args: V2WriterArgs) extends Actor with Logging {
  private val orc = new OrcContext(args.gcs, args.schemaProvider.ORCSchema,
    args.basePath, self.path.name, args.partitionBytes, args.pool)
  private val BatchSize = 1024
  private val reader = new ZReader(args.schemaProvider, BatchSize)
  private val errBuf = ByteBuffer.allocate(args.schemaProvider.LRECL * BatchSize)
  private val timer = new WriteTimer
  private var errorCount: Long = 0
  private var bytesIn: Long = 0

  def write(buf: ByteBuffer): Unit = {
    bytesIn += buf.limit
    val res = orc.write(reader, buf, errBuf)
    errorCount += res.errCount
    if (errBuf.position > 0){
      errBuf.flip()
      val a = new Array[Byte](args.schemaProvider.LRECL)
      errBuf.get(a)
      System.err.println(s"Failed to read row")
      System.err.println(PackedDecimal.hexValue(a))
    }
    if (orc.errPct > args.maxErrorPct)
      context.parent ! Protocol.PartFailed(s"errPct ${orc.errPct} > ${args.maxErrorPct}")
    else if (res.partFinished)
      context.parent ! Protocol.PartComplete(res.partPath, res.partBytes)
  }

  override def receive: Receive = {
    case Protocol.Start =>
      (0 until 10).foreach{_ => sender() ! args.pool.acquire()}

    case buf: ByteBuffer =>
      timer.start()
      write(buf)
      timer.end()

    case Protocol.Close =>
      timer.start()
      orc.close()
      timer.end()
      context.parent ! Protocol.FinishedWriting(bytesIn, orc.getBytesWritten)
      context.stop(self)

    case _ =>
  }

  override def postStop(): Unit = {
    timer.start()
    orc.close()
    timer.end()
    timer.close(logger,
      s"Stopping ${this.getClass.getSimpleName} ${self.path.name}",
      bytesIn, orc.getBytesWritten)
    val recordsIn = bytesIn / args.schemaProvider.LRECL
    val errorPct = errorCount*1.0d / recordsIn
    if (errorPct > args.maxErrorPct) {
      val msg = s"error percent $errorPct exceeds threshold of ${args.maxErrorPct}"
      logger.error(msg)
      context.parent ! Protocol.PartFailed(msg)
    }
  }
}
