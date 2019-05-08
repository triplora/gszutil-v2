package org.apache.spark.sql.execution.datasources.zfile

import com.google.cloud.gszutil.Decoding.{CopyBook, Decoder}
import com.google.cloud.gszutil.ZReader.ZIterator
import com.google.cloud.gszutil.ZOS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

class ZRowReader(private val copyBook: CopyBook) {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders.toArray
  private val lRecl: Int = decoders.foldLeft(0){_ + _.size}
  private val resultRow = new SpecificInternalRow(copyBook.getSchema)

  def readA(blk: Array[Byte], off: Int): InternalRow = {
    var i = 0
    var off = 0
    while (i < decoders.length){
      val decoder = decoders(i)
      decoder.get(blk, off, resultRow, i)
      off += decoder.size
      i += 1
    }
    resultRow
  }

  def readA(records: Iterator[(Array[Byte], Int)]): Iterator[InternalRow] =
    records.map(x => readA(x._1, x._2))

  def readA(dd: String): Iterator[InternalRow] =
    readA(new ZIterator(ZOS.readDD(dd)))
}
