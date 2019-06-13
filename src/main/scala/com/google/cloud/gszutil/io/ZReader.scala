package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Decoding.Decoder
import com.google.cloud.gszutil.Util.Logging
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.Writer

class ZReader(private val copyBook: CopyBook, private val batchSize: Int) extends Logging {
  private val decoders: Array[Decoder[_]] = copyBook.getDecoders
  private val nCols = decoders.length
  private val lRecl = copyBook.LRECL

  private def newBatch(): VectorizedRowBatch = {
    logger.debug("Creating new VectorizedRowBatch")
    val batch = new VectorizedRowBatch(nCols, batchSize)
    for (i <- decoders.indices)
      batch.cols(i) = decoders(i).columnVector(batchSize)
    batch
  }

  def readOrc(buf: ByteBuffer, writer: Writer): Unit = {
    while (buf.hasRemaining){
      writer.addRowBatch(readBatch(buf))
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

  private def readBatch(buf: ByteBuffer): VectorizedRowBatch = {
    val batch = newBatch()
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
