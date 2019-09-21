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

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.io.V2SendCallable.ReaderOpts
import com.google.cloud.gszutil.io.V2ReceiveCallable.ReceiverOpts
import com.google.common.util.concurrent.MoreExecutors
import org.scalatest.FlatSpec
import org.zeromq.ZContext

class ZChannelSpec extends FlatSpec {
  Util.configureLogging(true)

  "ZChannel" should "rw" in {
    val sendParallelism = 6
    val readExecutor = MoreExecutors.listeningDecorator(new ForkJoinPool(1))
    val executorService = MoreExecutors.listeningDecorator(new ForkJoinPool(1))
    val blkSize = 32*1024
    val inSize: Long = 1L*1024*1024*1024
    val timeout = 5
    val compress = true
    val ctx = new ZContext()
    val t0 = System.currentTimeMillis()
    val bufferPool = new BlockingBoundedBufferPool(blkSize, sendParallelism)
    val receiverOpts =
      ReceiverOpts(ctx, "127.0.0.1", 5570, blkSize, compress, bufferPool, null, null)
    val sink = executorService.submit(V2ReceiveCallable(receiverOpts))
    val readerOpts = ReaderOpts(new RandomBytes(inSize), blkSize, ctx, sendParallelism, "127.0.0.1", 5570)
    val src = readExecutor.submit(V2SendCallable(readerOpts))
    val readResult = src.get(timeout, TimeUnit.MINUTES)
    System.out.println(readResult)

    val writeResult = sink.get(timeout, TimeUnit.MINUTES)
    System.out.println(writeResult)
    assert(writeResult.isDefined)

    val bytesWritten = writeResult.get.bytesIn
    val bytesDecompressed = writeResult.get.bytesOut

    val t1 = System.currentTimeMillis()
    val dt = t1-t0
    val mbps = ((inSize*8d)/(1024L*1024L)) / (dt*0.001d - 2d)
    System.out.println(s"$mbps mbps")
    assert(mbps > 50)
    ctx.close()
    System.out.println(s"$bytesWritten bytes written in $dt ms")
    assert(readResult.get.bytesIn == inSize)
    System.out.println(s"$bytesDecompressed bytes decompressed")
    assert(bytesWritten > 0L, "expected bytes written > 0")
    assert(bytesDecompressed > 0L, "expected bytes decompressed > 0")
    assert(bytesDecompressed == inSize, "bytes decompressed should match input size")
  }

}
