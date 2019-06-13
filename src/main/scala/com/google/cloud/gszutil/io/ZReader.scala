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
  private val rowBatch: VectorizedRowBatch = {
    val batch = new VectorizedRowBatch(nCols, batchSize)
    for (i <- decoders.indices)
      batch.cols(i) = decoders(i).columnVector(batchSize)
    batch
  }

  def readOrc(buf: ByteBuffer, writer: Writer): Unit = {
    while (buf.hasRemaining) {
      val batch: VectorizedRowBatch = readBatch(buf)
      writer.addRowBatch(batch)
    }
  }

  /** Read
    *
    * @param buf byte array with multiple records
    * @param batch VectorizedRowBatch
    * @param rowId index within the batch
    */
  private def readRecord(buf: ByteBuffer,batch: VectorizedRowBatch, rowId: Int): Unit = {
    var i = 0
    while (i < decoders.length){
      decoders(i).get(buf, batch.cols(i), rowId)
      i += 1
    }
  }

  private def readBatch(buf: ByteBuffer): VectorizedRowBatch = {
    rowBatch.reset()
    var rowId = 0
    while (rowId < batchSize && buf.remaining() >= lRecl){
      val newPos = buf.position() + lRecl // TODO remove this
      readRecord(buf, rowBatch, rowId)
      assert(buf.position() == newPos) // TODO remove this
      rowId += 1
    }
    rowBatch.size = rowId
    if (rowBatch.size == 0)
      rowBatch.endOfFile = true
    rowBatch
  }
}
