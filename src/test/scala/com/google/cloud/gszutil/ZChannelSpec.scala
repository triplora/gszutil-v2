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

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import com.google.cloud.gszutil.io.Service
import com.google.common.util.concurrent.MoreExecutors
import org.zeromq.{SocketType, ZContext, ZMQ}
import org.scalatest.FlatSpec

class ZChannelSpec extends FlatSpec {
  "ZChannel" should "rw" in {
    val parallelism = 4
    val executorService = MoreExecutors.listeningDecorator(new ForkJoinPool(parallelism*2 + 2))
    val frontendUri = "tcp://127.0.0.1:5570"
    //val backendUri = "inproc://backend"
    val blkSize = 32*1024
    val inSize = 32 * blkSize

    val ctx = new ZContext()

    val sinkSocket = ctx.createSocket(SocketType.ROUTER)
    sinkSocket.bind(frontendUri)
    val sink = executorService.submit(new Service.Sink(sinkSocket))

    val t0 = System.currentTimeMillis()

    val sockets = (0 until parallelism).map{_ =>
      val socket = ctx.createSocket(SocketType.DEALER)
      socket.connect(frontendUri)
      socket.setLinger(-1)
      socket.setHWM(32)
      socket
    }.toArray

    val src = executorService.submit(
      new Service.Source(
        new Service.RandomBytes(inSize),
        blkSize,
        sockets))

    val bytesRead = src.get(30, TimeUnit.SECONDS).getBytesIn
    System.out.println(s"$bytesRead bytes read")

    (0 until parallelism*4).foreach{_ =>
      sockets.head.send(Array.empty[Byte], ZMQ.SNDMORE) // null frame removed by REP
      sockets.head.send(Array.empty[Byte], 0) // null frame indicating end of data
    }

    val bytesWritten = sink.get(30, TimeUnit.SECONDS).getBytesIn

    val t1 = System.currentTimeMillis()
    val dt = t1-t0
    ctx.close()
    System.out.println(s"$bytesWritten bytes written in $dt ms")
    assert(bytesWritten < bytesRead, "r/w bytes must match")
    assert(bytesRead == inSize)
  }

}
