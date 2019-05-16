package com.google.cloud.gszutil.io

import com.google.cloud.gszutil.Decoding.{CopyBook, Decoder}
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch

class ZReader(private val copyBook: CopyBook) {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders.toArray

  /** Read
    *
    * @param buf byte array with multiple records
    * @param recordStart offset within buffer
    * @param batch VectorizedRowBatch
    * @param rowId index within the batch
    */
  def readOrc(buf: Array[Byte], recordStart: Int, batch: VectorizedRowBatch, rowId: Int): Unit = {
    var fieldOffset = 0
    var i = 0
    while (i < decoders.length){
      val decoder = decoders(i)
      val off = recordStart + fieldOffset
      decoder.get(buf, off, batch.cols(i), rowId)
      fieldOffset += decoder.size
      i += 1
    }
  }

  def readOrc(data: Array[Byte], offset: Iterator[Int], batchSize: Int): Iterator[VectorizedRowBatch] = {
    new Iterator[VectorizedRowBatch]{
      private val columns = decoders.map(_.columnVector(batchSize))
      override def hasNext: Boolean = offset.hasNext
      override def next(): VectorizedRowBatch = {
        val batch = new VectorizedRowBatch(columns.length, batchSize)
        for (i <- columns.indices)
          batch.cols(i) = columns(i)
        var rowId = 0
        while (rowId < batchSize && offset.hasNext){
          readOrc(data, offset.next, batch, rowId)
          rowId += 1
        }
        batch.size = rowId
        if (!offset.hasNext)
          batch.endOfFile = true
        batch
      }
    }
  }

  def readOrc(in: ZRecordReaderT, maxSize: Int = 1024): Iterator[VectorizedRowBatch] = {
    val (data, offset) = ZIterator(in)
    readOrc(data, offset, maxSize)
  }
}
