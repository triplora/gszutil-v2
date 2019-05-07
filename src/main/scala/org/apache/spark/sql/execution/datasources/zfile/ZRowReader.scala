package org.apache.spark.sql.execution.datasources.zfile

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding.{CopyBook, Decoder}
import com.google.cloud.gszutil.ZReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

class ZRowReader(private val copyBook: CopyBook) {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders.toArray
  private val lRecl: Int = decoders.foldLeft(0){_ + _.size}
  private val buf: ByteBuffer = ByteBuffer.allocate(lRecl)
  private val resultRow = new SpecificInternalRow(copyBook.getSchema)

  private def readInternal(array: Array[Byte]): InternalRow = {
    buf.clear()
    buf.put(array)
    buf.flip()
    readInternal(buf)
  }

  private def readInternal(buf: ByteBuffer): InternalRow = {
    var i = 0
    while (i < decoders.length){
      val decoder = decoders(i)
      decoder.decodeInternal(buf, resultRow, i)
      i+=1
    }
    resultRow
  }

  def readDDInternal(ddName: String): Iterator[InternalRow] =
    readInternal(ZReader.readRecords(ddName))

  def readInternal(records: Iterator[Array[Byte]]): Iterator[InternalRow] =
    records.takeWhile(_ != null).map(readInternal)
}
