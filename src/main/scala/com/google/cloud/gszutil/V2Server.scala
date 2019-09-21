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

package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException

import akka.actor.{ActorSystem, Inbox, Props, Terminated}
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GCS
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{BlockingBoundedBufferPool, V2ActorArgs, V2ReceiveActor, V2ReceiveCallable}
import com.google.cloud.gszutil.orc.Protocol.{PartFailed, UploadComplete}
import com.google.cloud.storage.Storage
import com.google.common.base.Charsets
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.zeromq.ZContext

import scala.concurrent.duration.{DAYS, FiniteDuration, MINUTES}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object V2Server extends Logging {
  private val BatchSize = 1024
  private val PartSizeMB: Long = 128

  case class V2Config(host: String = "0.0.0.0",
                      port: Int = 8443,
                      gcs: Storage = GCS.defaultClient(GoogleCredentials
                        .getApplicationDefault
                        .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)),
                      destinationUri: String = "",
                      nWriters: Int = 4,
                      timeoutMinutes: Int = 300,
                      compress: Boolean = true,
                      maxErrorPct: Double = 0.00d)

  def main(args: Array[String]): Unit = {
    V2ConfigParser.parse(args) match {
      case Some(opts) =>
        run(opts)
    }
  }

  def run(config: V2Config): Result = {
    import config._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gReceiver", conf)
    val inbox = Inbox.create(sys)

    val ctx = new ZContext()
    val socket = V2ReceiveCallable.createSocket(ctx, host, port)

    val id = socket.recv(0)
    val init = socket.recv(0)
    val init2 = socket.recv(0)
    val init3 = socket.recv(0)
    val copyBook = CopyBook(new String(init, Charsets.UTF_8))
    val gcsUri = new String(init2, Charsets.UTF_8)
    val blkSize = ByteBuffer.wrap(init3).getInt

    val bufSize = copyBook.LRECL * BatchSize
    val pool = new BlockingBoundedBufferPool(bufSize, nWriters)

    val wArgs = V2ActorArgs(
      socket, blkSize, nWriters, copyBook,
      PartSizeMB*1024*1024, new Path(gcsUri),
      gcs, compress, pool, maxErrorPct)

    val reader = sys.actorOf(Props(classOf[V2ReceiveActor], wArgs), "DatasetReader")
    inbox.watch(reader)

    sys.whenTerminated.onComplete{
        case Success(_) =>
          logger.info(s"Actor System terminated with Success")
        case Failure(e) =>
          logger.info(s"ActorSystem terminated with Failure\n${e.getMessage}\n${e.getCause}")
    }(ExecutionContext.global)

    try {
      val timeout =
        if (timeoutMinutes > 0)
          FiniteDuration(timeoutMinutes, MINUTES)
        else
          FiniteDuration(1, DAYS)

      inbox.receive(timeout) match {
        case UploadComplete(read, written) =>
          logger.info(s"Upload complete:\n$read bytes read\n$written bytes written")
          Result.Success
        case PartFailed(msg) =>
          logger.error(s"Upload failed: $msg")
          Result.Failure(msg, 2)
        case x: Terminated =>
          val msg = s"${x.actor} terminated"
          logger.error(msg)
          Result.Failure(msg, 3)
        case msg =>
          val errMsg = s"Unrecognized message type ${msg.getClass.getSimpleName}: $msg"
          logger.error(errMsg)
          Result.Failure(errMsg)
      }
    } catch {
      case _: TimeoutException =>
        logger.error(s"Timed out after $timeoutMinutes minutes waiting for upload to complete")
        Result.Failure(s"Upload timed out after $timeoutMinutes minutes")
    } finally {
      sys.stop(reader)
      sys.stop(inbox.getRef)
      cleanup(sys)
    }
  }

  def cleanup(sys: ActorSystem): Unit = {
    logger.info("Cleaning up ActorSystem")
    sys.terminate()
    try {
      Await.result(sys.whenTerminated, atMost = FiniteDuration(1, MINUTES))
    } catch {
      case e: InterruptedException =>
        logger.warn("Interrupted waiting for ActorSystem cleanup")
      case e: TimeoutException =>
        logger.warn("Timed out waiting for ActorSystem cleanup")
    }
  }
}
