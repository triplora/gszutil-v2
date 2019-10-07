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
import java.util.concurrent.Callable

import akka.actor.ActorContext
import akka.io.BufferPool
import akka.routing.Router
import com.google.cloud.gszutil.Gzip
import com.google.cloud.gszutil.Util.Logging
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

object V2ReceiveCallable {
  val TwoMegaBytes: Int = 2*1024*1024
  val ReceiveQueueSize: Int = 8*1024

  def createSocket(ctx: ZContext, host: String, port: Int): Socket = {
    val socket = ctx.createSocket(SocketType.ROUTER)
    socket.bind(s"tcp://$host:$port")
    socket.setReceiveBufferSize(TwoMegaBytes)
    socket.setRcvHWM(ReceiveQueueSize)
    socket.setImmediate(false)
    socket.setIPv6(false)
    socket.setLinger(-1)
    socket
  }
}

class V2ReceiveCallable(socket: Socket, blkSize: Int, compress: Boolean, bufferPool: BufferPool,
                        router: Router, context: ActorContext)
  extends Callable[Option[ReceiveResult]] with Logging {

  override def call: Option[ReceiveResult] = {
    var bytesIn = 0L
    var msgCount = 0L
    var msgCount2 = 0L
    var bytesOut = 0L

    // Buffer to receive compressed data
    val inputBuffer = ByteBuffer.allocate(2*blkSize*256)

    try {
      while (!Thread.currentThread.isInterrupted) {
        val id = socket.recv(0)

        inputBuffer.clear()
        val bytesReceived = socket.recvByteBuffer(inputBuffer,0)
        msgCount += 1
        if (bytesReceived > 0) {
          msgCount2 += 1
          if (msgCount2 % 1000 == 0){
            logger.debug(s"Received $msgCount2")
          }
          if (compress) {
            // Buffer to receive decompressed data
            val outputBuffer = bufferPool.acquire()
            outputBuffer.clear()
            outputBuffer.put(Gzip.decompress(inputBuffer.array, 0, inputBuffer.limit))
            if (outputBuffer.position > 0) {
              bytesIn += inputBuffer.limit
              bytesOut += outputBuffer.position
              if (router != null && context != null) {
                outputBuffer.flip()
                router.route(outputBuffer, context.self)
              }
            }
          } else {
            bytesIn += inputBuffer.limit
          }
        } else {
          return Option(ReceiveResult(bytesIn, bytesOut, msgCount, msgCount2))
        }
      }
      Option(ReceiveResult(bytesIn, bytesOut, msgCount, msgCount2))
    } catch {
      case e: Exception =>
        logger.error("failed to receive", e)
        socket.close()
        None
    }
  }
}
