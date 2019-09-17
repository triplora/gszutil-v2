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
    val parallelism = 1
    val executorService = MoreExecutors.listeningDecorator(new ForkJoinPool(parallelism + 2))
    val frontEnd = "tcp://127.0.0.1:5570"
    val backend = "inproc://backend"
    val blkSize = 32*1024

    val ctx = new ZContext()

    executorService.submit(new Service.Router(ctx, 5570, backend))

    val sinks = (0 until parallelism).map{_ =>
      executorService.submit(new Service.Sink(ctx, backend))
    }

    val t0 = System.currentTimeMillis()

    val src = executorService.submit(new Service.Source(
      new Service.RandomBytes(32 * blkSize),
      blkSize))

    val bytesRead = src.get(1, TimeUnit.SECONDS).getBytesOut
    System.out.println(s"$bytesRead bytes read")

    val s = ctx.createSocket(SocketType.DEALER)
    s.connect(frontEnd)
    (0 until parallelism).foreach{_ =>
      s.send(Array.empty[Byte], ZMQ.SNDMORE) // null frame removed by REP
      s.send(Array.empty[Byte], 0) // null frame indicating end of data
    }

    val bytesWritten = sinks.foldLeft(0L){(a,b) => a + b.get(1, TimeUnit.SECONDS).getBytesIn}

    val t1 = System.currentTimeMillis()
    val dt = t1-t0
    System.out.println(s"$bytesWritten bytes written in $dt ms")
  }

}
