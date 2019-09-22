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

import java.util.concurrent.Callable
import java.util.zip.Inflater

import akka.actor.ActorContext
import akka.io.BufferPool
import akka.routing.Router
import com.google.cloud.gszutil.Util.Logging
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

object V2ReceiveCallable {
  val TwoMegaBytes: Int = 2*1024*1024
  val ReceiveQueueSize: Int = 8*1024
  case class ReceiverOpts(ctx: ZContext, host: String, port: Int, blkSize: Int, compress: Boolean,
                          bufferPool: BufferPool, router: Router, context: ActorContext)

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
    val inflater = new Inflater(true)

    try {
      while (!Thread.currentThread.isInterrupted) {
        val id = socket.recv(0)
        val data = socket.recv(0)
        msgCount += 1
        if (data != null && data.nonEmpty) {
          msgCount2 += 1
          if (compress) {
            inflater.reset()
            inflater.setInput(data, 0, data.length)
            val buf = bufferPool.acquire()
            buf.clear()
            val n = inflater.inflate(buf.array,0,buf.limit)
            if (n > 0) {
              buf.position(n)
              bytesIn += data.length
              bytesOut += n
              if (router != null)
                router.route(buf, context.self)
            }
          } else {
            bytesIn += data.length
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
