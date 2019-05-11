package com.google.cloud.gszutil.io

import com.google.cloud.gszutil.Decoding.{CopyBook, Decoder}
import com.google.cloud.gszutil.ZOS
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

class ZReader(private val copyBook: CopyBook) {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders.toArray
  private val resultRow = new SpecificInternalRow(copyBook.getSchema)

  def readA(blk: Array[Byte], recordStart: Int): InternalRow = {
    var fieldOffset = 0
    var i = 0
    while (i < decoders.length){
      val decoder = decoders(i)
      val off = recordStart + fieldOffset
      decoder.get(blk, off, resultRow, i)
      fieldOffset += decoder.size
      i += 1
    }
    resultRow
  }

  def readA(data: Array[Byte], offset: Iterator[Int]): Iterator[InternalRow] =
    offset.map(readA(data, _))

  def readA(dd: String): Iterator[InternalRow] = {
    val (data, offset) = ZIterator(ZOS.readDD(dd))
    readA(data, offset)
  }

  def readOrc(blk: Array[Byte], recordStart: Int, batch: VectorizedRowBatch): Unit = {
    var fieldOffset = 0
    var i = 0
    while (i < decoders.length){
      val decoder = decoders(i)
      val off = recordStart + fieldOffset
      decoder.get(blk, off, batch.cols(i), i)
      fieldOffset += decoder.size
      i += 1
    }
  }

  def readOrc(data: Array[Byte], offset: Iterator[Int], maxSize: Int): Iterator[VectorizedRowBatch] = {
    val batch = new VectorizedRowBatch(copyBook.getFieldNames.length, maxSize)
    batch.cols = copyBook.cols.toArray
    offset
      .grouped(maxSize)
      .map{indices =>
        indices.foreach(readOrc(data, _, batch))
        batch
      }
  }

  def readOrc(in: ZRecordReaderT, maxSize: Int = 1024): Iterator[VectorizedRowBatch] = {
    val (data, offset) = ZIterator(in)
    readOrc(data, offset, maxSize)
  }
}
