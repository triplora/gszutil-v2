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

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.imf.grecv.GRecvConfig
import com.google.cloud.imf.grecv.client.GRecvClient
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.{CloudLogging, Logging}
import org.scalatest.flatspec.AnyFlatSpec

class ZChannelSpec extends AnyFlatSpec with Logging {
  CloudLogging.configureLogging(debugOverride = true)

  "ZChannel" should "rw" in {
    val sendParallelism = 2
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
    val port = 51770
    val serverCfg = GRecvConfig(host, port)

    val in = new RandomBytes(inSize, blkSize, copyBook.LRECL)
    val request = GRecvRequest.newBuilder
      .setSchema(copyBook.toRecordBuilder.build)
      .setLrecl(in.lRecl)
      .setBlksz(in.blkSize)
      .setBasepath("gs://bucket/prefix")
      .setMaxErrPct(0)
      .build

    //val sendResult = GRecvClient.upload(request, host, port, 1, in)
    //assert(sendResult.exitCode == 0)
  }
}
