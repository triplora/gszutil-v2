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

import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.Decoding.Decoder
import com.google.cloud.gszutil.Util.Logging
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, VectorizedRowBatch, VoidColumnVector}
import org.apache.orc.Writer

/** Decodes MVS data set records into ORC row batch
  * @param schemaProvider SchemaProvider provides field decoders
  * @param batchSize size of ORC VectorizedRowBatch
  */
class ZReader(private val schemaProvider: SchemaProvider,
              private val batchSize: Int) extends Logging {
  private final val decoders: Array[Decoder] = schemaProvider.decoders
  private final val cols: Array[ColumnVector] = decoders.map{x =>
    x.columnVector(batchSize).getOrElse(new VoidColumnVector(batchSize))
  }
  private final val rowBatch: VectorizedRowBatch = {
    val batch = new VectorizedRowBatch(decoders.count(!_.filler), batchSize)
    var j = 0
    for (i <- decoders.indices) {
      if (!cols(i).isInstanceOf[VoidColumnVector]) {
        batch.cols(j) = cols(i)
        j += 1
      }
    }
    batch
  }

  /** Reads COBOL and writes ORC
    *
    * @param buf ByteBuffer containing data
    * @param writer ORC Writer
    * @param err ByteBuffer to receive rows with decoding errors
    * @return number of errors
    */
  def readOrc(buf: ByteBuffer, writer: Writer, err: ByteBuffer): (Long,Long) = {
    var errors: Long = 0
    var rows: Long = 0
    while (buf.remaining >= schemaProvider.LRECL) {
      rowBatch.reset()
      val (rowId,errCt) = ZReader.readBatch(buf,decoders,cols,batchSize,schemaProvider.LRECL,err)
      if (rowId == 0)
        rowBatch.endOfFile = true
      rowBatch.size = rowId
      rows += rowId
      errors += errCt
      writer.addRowBatch(rowBatch)
    }
    (rows,errors)
  }
}

object ZReader {
  /**
    *
    * @param buf ByteBuffer with position at column to be decoded
    * @param decoder Decoder instance
    * @param col ColumnVector to receive decoded value
    * @param rowId index within ColumnVector to store decoded value
    */
  private final def readColumn(buf: ByteBuffer, decoder: Decoder, col: ColumnVector, rowId: Int)
  : Unit = {
    decoder.get(buf, col, rowId)
  }

  /** Read
    * @param buf input ByteBuffer with position set to start of record
    * @param decoders Array[Decoder] to read from input
    * @param cols Array[ColumnVector] to receive Decoder output
    * @param rowId index within the batch
    */
  private final def readRecord(buf: ByteBuffer,
                               decoders: Array[Decoder],
                               cols: Array[ColumnVector],
                               rowId: Int): Unit = {
    var i = 0
    while (i < decoders.length){
      readColumn(buf, decoders(i), cols(i), rowId)
      i += 1
    }
  }

  /**
    *
    * @param buf ByteBuffer containing data
    * @param decoders Decoder instances
    * @param cols VectorizedRowBatch
    * @param batchSize rows per batch
    * @param lRecl size of each row in bytes
    * @param err ByteBuffer to receive failed rows
    * @return number of errors encountered
    */
  private final def readBatch(buf: ByteBuffer,
                              decoders: Array[Decoder],
                              cols: Array[ColumnVector],
                              batchSize: Int,
                              lRecl: Int,
                              err: ByteBuffer): (Int,Int) = {
    err.clear()
    var errors: Int = 0
    var rowId = 0
    var rowStart = 0
    val errRecord = new Array[Byte](lRecl)
    while (rowId < batchSize && buf.remaining >= lRecl){
      rowStart = buf.position()
      try {
        readRecord(buf, decoders, cols, rowId)
        rowId += 1
      } catch {
        case _: IllegalArgumentException =>
          errors += 1
          buf.position(rowStart)
          buf.get(errRecord)
          err.put(errRecord)
      }
    }
    (rowId,errors)
  }
}
