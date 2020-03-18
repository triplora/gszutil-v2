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

import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Callable, TimeoutException}

import akka.actor.{ActorSystem, Inbox, Props, Terminated}
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GCS
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.V2Server.V2Config
import com.google.cloud.gszutil.io.{BlockingBoundedBufferPool, V2ActorArgs, V2ReceiveActor, V2ReceiveCallable, V2SendCallable}
import com.google.cloud.gszutil.orc.Protocol
import com.google.cloud.gszutil.orc.Protocol.{PartFailed, UploadComplete}
import com.google.cloud.gzos.Ebcdic
import com.google.cloud.gzos.pb.Schema.Record
import com.google.cloud.storage.Storage
import com.google.common.base.{Charsets, Preconditions}
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.zeromq.{ZContext, ZMQ}

import scala.concurrent.duration.{DAYS, FiniteDuration, MINUTES}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object V2Server extends Logging {
  val BatchSize = 1024
  val PartitionBytes: Long = 128L * 1024 * 1024

  case class V2Config(host: String = "127.0.0.1",
                      port: Int = 5570,
                      nWriters: Int = 4,
                      bufCt: Int = 64,
                      timeoutMinutes: Int = 300,
                      compress: Boolean = true,
                      daemon: Boolean = true,
                      maxErrorPct: Double = 0.00d,
                      gcs: Storage = GCS.defaultClient(GoogleCredentials
                        .getApplicationDefault
                        .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)))

  def main(args: Array[String]): Unit = {
    System.out.println("Build Info:\n" + Util.readS("build.txt"))
    Util.configureLogging(true)
    V2ConfigParser.parse(args) match {
      case Some(opts) =>
        val server = new V2Server(opts)
        if (opts.daemon){
          while (!Thread.currentThread().isInterrupted){
            val rc = server.call()
            System.out.println(s"V2Server instance returned exit code $rc")
          }
        } else {
          val rc = server.call()
          System.out.println(s"V2Server instance returned exit code $rc")
          System.exit(rc.exitCode)
        }
      case _ =>
        System.err.println(s"Unabled to parse args '${args.mkString(" ")}'")
        System.exit(1)
    }
  }

  def readId(frame: Array[Byte]): Int = {
    val idBuf = ByteBuffer.wrap(frame)
    idBuf.get()
    idBuf.getInt
  }
}

class V2Server(config: V2Config) extends Callable[Result] with Logging {
  override def call(): Result = {
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    logger.info("Initializing ActorSystem")
    val sys = ActorSystem("grecv", conf)
    val inbox = Inbox.create(sys)

    val ctx = new ZContext()
    val socket = V2ReceiveCallable.createSocket(ctx, config.host, config.port)

    // Receive Session Details
    logger.info(s"Waiting to receive session details on tcp://${config.host}:${config.port} ...")
    val frame1 = socket.recv(0)
    socket.hasReceiveMore
    logger.debug(s"Received ID frame ${V2Server.readId(frame1)}\n" + PackedDecimal.hexValue(frame1))

    val frame1a = socket.recv(0)
    logger.debug(s"Received BEGIN frame\n" + PackedDecimal.hexValue(frame1a))
    require(util.Arrays.equals(frame1a,Protocol.Begin), "expected BEGIN message")

    // Record proto message
    val frame2 = socket.recv(0)
    val record = Record.parseFrom(frame2)

    val schema: SchemaProvider =
      if (record.getSource == Record.Source.COPYBOOK)
        CopyBook(record.getOriginal, if (record.getEncoding == "") Ebcdic else Utf8)
      else if (record.getSource == Record.Source.LAYOUT)
        RecordSchema(record)
      else throw new NotImplementedError()

    logger.info(s"Received Record Schema\n```$schema```\n")

    // GCS Prefix
    val frame3 = socket.recv(0)
    val gcsUri = new String(frame3, Charsets.UTF_8).stripSuffix("/")
    logger.info(s"Received GCS URI $gcsUri")
    val uri = new URI(gcsUri)
    require(uri.getScheme == "gs", "scheme must be gs")
    require(uri.getAuthority.nonEmpty, "bucket must be provided")

    // Block Size
    val frame4 = socket.recv(0)
    val blkSize = ByteBuffer.wrap(frame4).getInt
    logger.info(s"Received Block Size $blkSize")

    val bufSize = 2 * blkSize
    logger.info(s"Allocating Buffer Pool with size ${1L*bufSize*config.nWriters*config.bufCt}")
    val pool = new BlockingBoundedBufferPool(bufSize, config.nWriters*config.bufCt)

    val wArgs = V2ActorArgs(socket, blkSize, config.nWriters, schema, V2Server.PartitionBytes,
      new Path(gcsUri), config.gcs, config.compress,
      pool, config.maxErrorPct, inbox.getRef())

    val reader = sys.actorOf(Props(classOf[V2ReceiveActor], wArgs), "DatasetReader")
    inbox.watch(reader)

    // Send ACK to indicate server is ready to receive data
    logger.info("Sending ACK")
    socket.send(frame1,ZMQ.SNDMORE) // sender identity
    socket.send(Protocol.Ack,ZMQ.SNDMORE) // ACK message
    logger.info(s"Sending LRECL ${schema.LRECL}")
    socket.send(V2SendCallable.encodeInt(schema.LRECL),0) // LRECL confirmation
    logger.info("Waiting to receive data")

    // Set termination callback
    sys.whenTerminated.onComplete{
        case Success(_) =>
          logger.info(s"Actor System terminated with Success")
        case Failure(e) =>
          logger.info(s"ActorSystem terminated with Failure\n${e.getMessage}\n${e.getCause}")
    }(ExecutionContext.global)

    try {
      val timeout =
        if (config.timeoutMinutes > 0)
          FiniteDuration(config.timeoutMinutes, MINUTES)
        else
          FiniteDuration(1, DAYS)

      logger.info(s"ActorSystem timeout is $timeout")
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
        val msg = s"Upload timed out after ${config.timeoutMinutes} minutes"
        logger.error(msg)
        Result.Failure(msg)
    } finally {
      sys.stop(reader)
      sys.stop(inbox.getRef)
      ctx.close()
      cleanup(sys)
    }
  }

  def cleanup(sys: ActorSystem): Unit = {
    logger.info("Cleaning up ActorSystem")
    sys.terminate()
    try {
      Await.result(sys.whenTerminated, atMost = FiniteDuration(1, MINUTES))
    } catch {
      case _: InterruptedException =>
        logger.warn("Interrupted waiting for ActorSystem cleanup")
      case _: TimeoutException =>
        logger.warn("Timed out waiting for ActorSystem cleanup")
    }
  }
}
