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
import java.util.concurrent.Callable
import java.util.zip.Deflater

import com.google.cloud.gszutil.Util.Logging
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

object V2SendCallable {
  case class ReaderOpts(in: ReadableByteChannel, blkSize: Int,
                        ctx: ZContext, nConnections: Int, host: String, port: Int)
  def apply(opts: ReaderOpts): V2SendCallable = {
    import opts._
    val uri = s"tcp://$host:$port"
    val sockets = (1 to nConnections).map{ i =>
      val socket = ctx.createSocket(SocketType.DEALER)
      socket.setSendBufferSize(256*1024)
      socket.setIPv6(false)
      socket.setImmediate(false)
      socket.connect(uri)
      socket.setLinger(-1)
      socket.setHWM(1024)
      val identity = ByteBuffer.allocate(5)
      identity.put(0.toByte)
      identity.putInt(i)
      socket.setIdentity(identity.array())
      socket
    }
    new V2SendCallable(in, blkSize, sockets)
  }
}

/** Compresses blocks with GZIP
  * Sends using multiple sockets
  * Runs on a single thread
  */
final class V2SendCallable(in: ReadableByteChannel, blkSize: Int, sockets: Seq[Socket])
  extends Callable[Option[SendResult]] with AutoCloseable with Logging {

  override def call(): Option[SendResult] = {
    var bytesIn = 0L
    var bytesOut = 0L
    var msgCount = 0L
    val data = new Array[Byte](blkSize)
    val bb = ByteBuffer.wrap(data)

    val buf = new Array[Byte](blkSize)
    val deflater = new Deflater(3, true)
    deflater.reset()

    var i: Int = 0
    try {
      while (!Thread.currentThread.isInterrupted) {
        bb.clear

        // Read from input dataset
        if (in.read(bb) < 0) {
          logger.info(s"Input exhausted after $bytesIn bytes $msgCount messages")
          close()
          return Option(SendResult(bytesIn, bytesOut, msgCount))
        }

        if (bb.position > 0) {
          bytesIn += bb.position

          // Load deflater
          deflater.setInput(data, 0, bb.position)
          deflater.finish()

          // Compress bytes
          val compressed = deflater.deflate(buf, 0, buf.length, Deflater.FULL_FLUSH)
          deflater.reset()
          if (compressed > 0) {
            // Socket round-robin
            i += 1
            if (i >= sockets.length) i = 0

            // Send payload
            sockets(i).send(buf,0, compressed, 0)

            // Increment counters
            bytesOut += compressed
            msgCount += 1
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

  override def close(): Unit = {
    val n = sockets.length
    if (n > 1) {
      logger.info(s"Closing ${n-1} of $n sockets...")
      sockets.tail.foreach(_.close())
    }

    // Allow time for queues to be flushed
    Thread.sleep(2000L)

    logger.info("Sending null frames to signal end of data")
    sockets.head.send(Array.empty[Byte], 0)
    sockets.head.close()
  }
}
