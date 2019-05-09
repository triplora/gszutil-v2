package com.google.cloud.gszutil.io

import com.google.cloud.gszutil.Decoding.{CopyBook, Decoder}
import com.google.cloud.gszutil.ZOS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

class ZRowReader(private val copyBook: CopyBook) {
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
}
