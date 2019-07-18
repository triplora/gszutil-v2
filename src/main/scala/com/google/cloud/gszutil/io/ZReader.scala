/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Decoding.Decoder
import com.google.cloud.gszutil.Util.Logging
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.Writer

/** Uses a Copy Book to convert MVS data set records into an ORC row batch
  *
  * @param copyBook
  * @param batchSize
  */
class ZReader(private val copyBook: CopyBook, private val batchSize: Int) extends Logging {
  private final val decoders: Array[Decoder] = copyBook.decoders
  private final val rowBatch: VectorizedRowBatch = {
    val batch = new VectorizedRowBatch(copyBook.decoders.length, batchSize)
    for (i <- decoders.indices) {
      batch.cols(i) = decoders(i).columnVector(batchSize)
    }
    batch
  }

  def readOrc(buf: ByteBuffer, writer: Writer): Unit = {
    while (buf.remaining >= copyBook.LRECL) {
      ZReader.readBatch(decoders, buf, rowBatch, batchSize, copyBook.LRECL)
      writer.addRowBatch(rowBatch)
    }
  }
}

object ZReader {
  /** Read
    *
    * @param buf byte array with multiple records
    * @param rowId index within the batch
    */
  private final def readRecord(decoders: Array[Decoder], buf: ByteBuffer, rowBatch: VectorizedRowBatch, rowId: Int): Unit = {
    var i = 0
    while (i < decoders.length){
      decoders(i).get(buf, rowBatch.cols(i), rowId)
      i += 1
    }
  }

  private final def readBatch(decoders: Array[Decoder], buf: ByteBuffer, rowBatch: VectorizedRowBatch, batchSize: Int, lRecl: Int): Unit = {
    rowBatch.reset()
    var rowId = 0
    while (rowId < batchSize && buf.remaining >= lRecl){
      readRecord(decoders, buf, rowBatch, rowId)
      rowId += 1
    }
    rowBatch.size = rowId
    if (rowBatch.size == 0)
      rowBatch.endOfFile = true
  }
}
