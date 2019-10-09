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
import java.nio.channels.ReadableByteChannel
import java.util
import java.util.concurrent.Callable
import java.util.zip.Deflater

import com.google.cloud.gszutil.{CopyBook, PackedDecimal}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.orc.Protocol
import com.google.common.base.Charsets
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext, ZMQ}

object V2SendCallable extends Logging {
  final val FiveMinutesInMillis: Int = 5*60*1000
  final val TargetBlocks = 128
  final val GzipBufferSize = 32*1024
  case class ReaderOpts(in: ReadableByteChannel,
                        copyBook: CopyBook,
                        gcsUri: String,
                        blkSize: Int,
                        ctx: ZContext, nConnections: Int, host: String, port: Int)

  /** Opens sockets and sends session details
    * blocks until the server responds with ACK
    *
    * @param opts ReaderOpts
    * @return V2SendCallable
    */
  def apply(opts: ReaderOpts): V2SendCallable = {
    val uri = s"tcp://${opts.host}:${opts.port}"
    logger.debug(s"Opening ${opts.nConnections} connections to $uri...")
    val n = math.min(1,opts.nConnections)
    val sockets = (1 to n).map{i =>
      val socket = opts.ctx.createSocket(SocketType.DEALER)
      socket.setSendBufferSize(256*1024)
      socket.setIPv6(false)
      socket.setImmediate(false)
      socket.connect(uri)
      socket.setLinger(-1)
      socket.setHWM(1024)
      socket.setIdentity(mkId(i))
      socket
    }
    val socket = sockets.head
    socket.setReceiveTimeOut(FiveMinutesInMillis)
    logger.debug(s"Sending BEGIN\n${PackedDecimal.hexValue(Protocol.Begin)}")
    socket.send(Protocol.Begin, ZMQ.SNDMORE)
    logger.debug(s"CopyBook with LRECL ${opts.copyBook.LRECL}")
    socket.send(opts.copyBook.raw.getBytes(Charsets.UTF_8), ZMQ.SNDMORE)
    logger.debug(s"GCS prefix ${opts.gcsUri}")
    socket.send(opts.gcsUri.getBytes(Charsets.UTF_8), ZMQ.SNDMORE)
    logger.debug(s"Block size ${opts.blkSize}")
    socket.send(encodeInt(opts.blkSize), 0)
    logger.debug("Waiting for ACK")
    val ack = socket.recv(0)
    require(socket.hasReceiveMore, "expected more frames")
    val frame2 = socket.recv(0)
    val lreclConfirmation = decodeInt(frame2)
    if (ack == null) {
      throw new RuntimeException("Timed out waiting for ACK from Receiver")
    } else if (util.Arrays.equals(ack, Protocol.Ack) && lreclConfirmation == opts
      .copyBook
      .LRECL){
      logger.debug(s"Received ACK and LRECL $lreclConfirmation = ${opts.copyBook.LRECL}")
      new V2SendCallable(opts.in, opts.blkSize, sockets)
    } else {
      throw new RuntimeException("Received invalid ACK message: $ack")
    }
  }

  def encodeInt(x: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(x)
    buf.array()
  }

  def decodeInt(frame: Array[Byte]): Int = {
    val buf = ByteBuffer.wrap(frame)
    buf.getInt
  }

  def encodeLong(x: Long): Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    buf.putLong(x)
    buf.array()
  }

  def decodeLong(frame: Array[Byte]): Long = {
    val buf = ByteBuffer.wrap(frame)
    buf.getLong
  }

  private def encodeIdentity(x: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(5)
    buf.put(0.toByte)
    buf.putInt(x)
    buf.array()
  }

  private def mkId(i: Int): Array[Byte] = {
    val identity = ByteBuffer.allocate(5)
    identity.put(0.toByte)
    identity.putInt(i)
    identity.array()
  }
}

/** Compresses blocks with GZIP
  * Sends using multiple sockets
  * Runs on a single thread
  */
final class V2SendCallable(in: ReadableByteChannel, blkSize: Int, sockets: Seq[Socket])
  extends Callable[Option[SendResult]] with Logging {

  override def call(): Option[SendResult] = {
    var bytesIn = 0L
    var bytesOut = 0L
    var bytesRead = 0L
    var recordCount = 0L
    var msgCount = 0L
    var yieldCount = 0L
    var socketId = 0
    val readBuf = ByteBuffer.allocate(blkSize)
    val compressBuf = new Array[Byte](blkSize*2)
    val outputBuffer = ByteBuffer.allocate(compressBuf.length)
    val deflater = new Deflater(3, true)
    deflater.reset()

    try {
      while (!Thread.currentThread.isInterrupted) {
        readBuf.clear

        // Read from input dataset
        bytesRead = in.read(readBuf)
        if (bytesRead < 0) {
          logger.info(s"Input exhausted after $bytesIn bytes $msgCount messages")
          val rc = finish(msgCount)
          return Option(SendResult(bytesIn, bytesOut, msgCount, yieldCount, rc))
        } else if (bytesRead == 0) {
          yieldCount += 1
          Thread.`yield`()
        }

        while (readBuf.hasRemaining && bytesRead > -1){
          bytesRead = in.read(readBuf)
          if (bytesRead > 0) {
            recordCount += 1
          } else if (bytesRead == 0) {
            yieldCount += 1
            Thread.`yield`()
          }
        }

        if (readBuf.position > 0) {
          bytesIn += readBuf.position

          // Compress bytes
          deflater.setInput(readBuf.array, 0, readBuf.position)
          deflater.finish()
          val compressed = deflater.deflate(compressBuf, 0, compressBuf.length, Deflater.FULL_FLUSH)
          deflater.reset()

          if (compressed > 0) {
            outputBuffer.clear()
            outputBuffer.put(compressBuf, 0, compressed)
            outputBuffer.flip()

            // Socket round-robin
            socketId += 1
            if (socketId >= sockets.length) socketId = 0

            // Send payload
            val bytesQueued = sockets(socketId).sendByteBuffer(outputBuffer, 0)
            if (bytesQueued < 0){
              val rc = 1
              logger.error("Failed to enqueue bytes")
              return Option(SendResult(bytesIn, bytesOut, msgCount, yieldCount, rc))
            }

            // Increment counters
            bytesOut += bytesQueued
            msgCount += 1
            if (msgCount % 10000 == 0){
              logger.info("blocks sent: " + msgCount)
            }
          }
        }
      }
      logger.warn("thread was interrupted")
      None
    } catch {
      case e: Exception =>
        logger.error("Failed to upload data", e)
        None
    }
  }

  def finish(msgCount: Long): Int = {
    val n = sockets.length
    if (n > 1) {
      logger.info(s"Closing ${n-1} of $n sockets...")
      sockets.tail.foreach(_.close())
    }

    // Allow time for queues to be flushed
    Thread.sleep(2000L)

    logger.info("Sending null frames to signal end of data")
    val socket = sockets.head
    socket.send(Array.empty[Byte], 0)

    logger.info("Waiting for FIN message...")
    socket.setReceiveTimeOut(30000) // Wait for up to 30 seconds
    val closeMsg = socket.recv(0)
    if (socket.hasReceiveMore){
      val frame2 = socket.recv()
      if (frame2 != null){
        val msgRecvCount = V2SendCallable.decodeLong(frame2)
        if (msgRecvCount != msgCount) {
          logger.error(s"Server received $msgRecvCount messages but $msgCount were sent")
          socket.close()
          return 1
        } else {
          logger.info(s"Server received $msgRecvCount messages")
        }
      }
    } else {
      logger.warn("Server didn't send received message count")
    }
    socket.close()
    if (closeMsg == null){
      logger.error("Server did not send FIN message within 30s timeout period")
      1
    } else if (!util.Arrays.equals(closeMsg, Protocol.Fin)) {
      logger.error(s"expected FIN but received message with length ${closeMsg.length}")
      1
    } else {
      logger.info("Received FIN")
      0
    }
  }
}
