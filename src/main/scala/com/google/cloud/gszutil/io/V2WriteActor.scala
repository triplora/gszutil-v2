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
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.orc.Protocol

final class V2WriteActor(args: V2WriterArgs) extends Actor with Logging {
  val core = new WriterCore(args, self.path.name)

  override def receive: Receive = {
    case Protocol.Start =>
      (0 until 10).foreach{_ => sender() ! args.pool.acquire()}

    case buf: ByteBuffer =>
      val res = core.write(buf)
      if (core.orc.errPct > args.maxErrorPct)
        context.parent ! Protocol.PartFailed(s"errPct ${core.orc.errPct} > ${args.maxErrorPct}")
      else if (res.partFinished)
        context.parent ! Protocol.PartComplete(res.partPath, res.partBytes)

    case Protocol.Close =>
      core.orc.close()
      context.parent ! Protocol.FinishedWriting(core.getBytesIn, core.orc.getBytesWritten)
      context.stop(self)

    case _ =>
  }

  override def postStop(): Unit = {
    core.orc.close()
    val recordsIn = core.getBytesIn / args.schemaProvider.LRECL
    val errorPct = core.getErrorCount*1.0d / recordsIn
    if (errorPct > args.maxErrorPct) {
      val msg = s"error percent $errorPct exceeds threshold of ${args.maxErrorPct}"
      logger.error(msg)
      context.parent ! Protocol.PartFailed(msg)
    }
  }
}
