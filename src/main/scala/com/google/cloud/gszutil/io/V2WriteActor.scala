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
import com.google.cloud.gszutil.orc.Protocol.PartFailed

/** Responsible for writing a single output partition
  */
final class V2WriteActor(args: V2ActorArgs) extends Actor with Logging {
  private val orc = new OrcContext(args.gcs, args.copyBook.ORCSchema, args.compress,
    args.path, self.path.name, args.partitionBytes, args.pool)
  private val BatchSize = 1024
  private val codec = new ZReader(args.copyBook, BatchSize)
  private val errBuf = ByteBuffer.allocate(args.copyBook.LRECL * BatchSize)
  private val timer = new WriteTimer
  private var errorCount: Long = 0
  private var bytesIn: Long = 0

  private def write(buf: ByteBuffer): Unit = {
    bytesIn += buf.limit
    errorCount += orc.write(codec, buf, errBuf)
    if (errBuf.position > 0){
      errBuf.flip()
      val a = new Array[Byte](args.copyBook.LRECL)
      errBuf.get(a)
      System.err.println(s"Failed to read row:\n${PackedDecimal.hexValue(a)}")
    }
    if (buf.remaining > 0) {
      logger.warn(s"Discarding ${buf.remaining} bytes remaining in buffer. This should never " +
        "happen for a normal MVS data set with fixed-length records")
    }
  }

  override def receive: Receive = {
    case buf: ByteBuffer =>
      timer.timed(() => write(buf))

    case s: String if s == "finished" =>
      orc.close()
      context.parent ! Protocol.FinishedWriting(bytesIn, orc.getBytesWritten)
      context.stop(self)

    case _ =>
  }

  override def postStop(): Unit = {
    timer.timed(() => orc.close())
    timer.close(logger,
      s"Stopping ${this.getClass.getSimpleName} ${self.path.name}",
      bytesIn, orc.getBytesWritten)
    val recordsIn = bytesIn / args.copyBook.LRECL
    val errorPct = errorCount*1.0d / recordsIn
    if (errorPct > args.maxErrorPct) {
      val msg = s"error percent $errorPct exceeds threshold of ${args.maxErrorPct}"
      logger.error(msg)
      context.parent ! PartFailed(msg)
    }
  }
}
