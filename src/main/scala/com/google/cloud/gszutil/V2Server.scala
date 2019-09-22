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
import com.google.cloud.gszutil.orc.Protocol
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
  private val PartitionBytes: Long = 128L*1024*1024

  case class V2Config(host: String = "0.0.0.0",
                      port: Int = 8443,
                      gcs: Storage = GCS.defaultClient(GoogleCredentials
                        .getApplicationDefault
                        .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)),
                      destinationUri: String = "",
                      nWriters: Int = 8,
                      bufCt: Int = 256,
                      timeoutMinutes: Int = 300,
                      compress: Boolean = true,
                      maxErrorPct: Double = 0.00d)

  def main(args: Array[String]): Unit = {
    V2ConfigParser.parse(args) match {
      case Some(opts) =>
        val rc = run(opts)
        System.exit(rc.exitCode)
      case _ =>
        System.err.println(s"Unabled to parse args '${args.mkString(" ")}'")
        System.exit(1)
    }
  }

  def run(config: V2Config): Result = {
    import config._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    logger.debug("Initializing ActorSystem")
    val sys = ActorSystem("grecv", conf)
    val inbox = Inbox.create(sys)

    val ctx = new ZContext()
    val socket = V2ReceiveCallable.createSocket(ctx, host, port)

    // Collect Send Options
    logger.debug("Waiting to receive session details...")
    val frame1 = socket.recv(0) // sender identity
    val frame2 = socket.recv(0) // copy book text
    val frame3 = socket.recv(0) // GCS URI prefix
    val frame4 = socket.recv(0) // blkSize
    val copyBook = CopyBook(new String(frame2, Charsets.UTF_8))
    val gcsUri = new String(frame3, Charsets.UTF_8)
    val blkSize = ByteBuffer.wrap(frame4).getInt

    logger.debug(s"Allocating Buffer Pool with size ${blkSize*nWriters*config.bufCt}")
    val pool = new BlockingBoundedBufferPool(blkSize, nWriters*config.bufCt)

    val wArgs = V2ActorArgs(socket, blkSize, nWriters, copyBook, PartitionBytes,
      new Path(gcsUri), gcs, compress, pool, maxErrorPct, inbox.getRef())

    val reader = sys.actorOf(Props(classOf[V2ReceiveActor], wArgs), "DatasetReader")
    inbox.watch(reader)

    // Send ACK to indicate server is ready to receive data
    logger.debug("Sending ACK")
    socket.send(Protocol.Ack.toArray,0);

    // Set termination callback
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

      logger.debug("Waiting for ActorSystem termination...")
      inbox.receive(timeout) match {
        case UploadComplete(read, written) =>
          logger.info(s"Upload complete:\n$read bytes read\n$written bytes written")
          Result.Success
        case Protocol.Failed =>
          logger.error(s"Upload failed")
          Result.Failure("Upload failed", 2)
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
