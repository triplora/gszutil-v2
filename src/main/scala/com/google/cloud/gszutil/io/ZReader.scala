package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Decoding.Decoder
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.Writer

class ZReader(private val copyBook: CopyBook) {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders.toArray
  private val nCols = decoders.length
  private val lRecl = copyBook.LRECL

  def readOrc(buf: ByteBuffer, writer: Writer, batchSize: Int): Unit = {
    while (buf.hasRemaining){
      writer.addRowBatch(readBatch(buf, batchSize))
    }
  }

  /** Read
    *
    * @param buf byte array with multiple records
    * @param recordStart offset within buffer
    * @param batch VectorizedRowBatch
    * @param rowId index within the batch
    */
  private def readRecord(buf: Array[Byte], recordStart: Int, batch: VectorizedRowBatch, rowId: Int): Unit = {
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

  private def readBatch(buf: ByteBuffer, batchSize: Int): VectorizedRowBatch = {
    val columns = decoders.map(_.columnVector(batchSize))
    val batch = new VectorizedRowBatch(nCols, batchSize)
    for (i <- columns.indices)
      batch.cols(i) = columns(i)
    var rowId = 0
    while (rowId < batchSize && buf.remaining() >= lRecl){
      readRecord(buf.array(), buf.position(), batch, rowId)
      val newPos = buf.position() + lRecl
      buf.position(newPos)
      rowId += 1
    }
    batch.size = rowId
    batch.endOfFile = true
    batch
  }
}
