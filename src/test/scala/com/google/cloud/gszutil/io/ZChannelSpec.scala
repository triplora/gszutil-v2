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

import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.V2Server.V2Config
import com.google.cloud.gszutil.io.V2SendCallable.ReaderOpts
import com.google.cloud.gszutil.{CopyBook, Util, V2Server}
import com.google.common.util.concurrent.MoreExecutors
import org.scalatest.FlatSpec
import org.zeromq.ZContext

class ZChannelSpec extends FlatSpec with Logging {
  Util.configureLogging(true)

  "ZChannel" should "rw" in {
    val sendParallelism = 6
    val readExecutor = MoreExecutors.listeningDecorator(new ForkJoinPool(2))
    val ctx = new ZContext()

    val gcsUri = "gs://gsztest/grecv-test/"
    val copyBook = CopyBook(
      """    01  TEST-LAYOUT-FIVE.
        |        03  COL-A                    PIC S9(9) COMP.
        |        03  COL-B                    PIC S9(4) COMP.
        |        03  COL-C                    PIC S9(4) COMP.
        |        03  COL-D                    PIC X(01).
        |        03  COL-E                    PIC S9(9) COMP.
        |        03  COL-F                    PIC S9(07)V9(2) COMP-3.
        |        03  COL-G                    PIC S9(05)V9(4) COMP-3.
        |        03  COL-H                    PIC S9(9) COMP.
        |        03  COL-I                    PIC S9(9) COMP.
        |        03  COL-J                    PIC S9(4) COMP.
        |        03  COL-K                    PIC S9(16)V9(2) COMP-3.
        |        03  COL-L                    PIC S9(16)V9(2) COMP-3.
        |        03  COL-M                    PIC S9(16)V9(2) COMP-3.
        |""".stripMargin)

    val blkSize: Int = 32*1024 - (32*1024 % copyBook.LRECL)
    val inSize: Long = 256L*blkSize
    val host = "127.0.0.1"
    val port = 5570
    val serverCfg = V2Config(host, port)
    val recvResult = readExecutor.submit(new V2Server(serverCfg))

    val readerOpts = ReaderOpts(new RandomBytes(inSize, blkSize, copyBook.LRECL), copyBook, gcsUri,
      blkSize,
      ctx, sendParallelism, host, port, 1024)

    val sendResult = V2SendCallable(readerOpts).call()
    assert(sendResult.isDefined)
    assert(sendResult.get.rc == 0)
    ctx.close()
    assert(sendResult.get.bytesIn == inSize)

    recvResult.get(60, TimeUnit.SECONDS)
    assert(recvResult.isDone)
    assert(recvResult.get.exitCode == 0)
  }

}
