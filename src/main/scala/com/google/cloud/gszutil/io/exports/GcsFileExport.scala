package com.google.cloud.gszutil.io.exports


import com.google.cloud.WriteChannel
import com.google.cloud.bqsh.cmd.Scp.blobId
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.{BlobInfo, Storage}

import java.nio.ByteBuffer

case class GcsFileExport(gcs: Storage, gcsOutUri: String, lrecl: Int) extends FileExport with Logging {
  val recfm: String = "FB"
  val lRecl: Int = lrecl
  private lazy val writer: WriteChannel = {
    val result = gcs.writer(
      BlobInfo.newBuilder(blobId(gcsOutUri))
        .setContentType("application/octet-stream")
        .setContentEncoding("identity")
        .build)
    result.setChunkSize(5 * 1024 * 1024) //when to flush to GCS
    result
  }

  var rowsWritten: Long = 0L

  override def appendBytes(buf: Array[Byte]): Unit = {
    writer.write(ByteBuffer.wrap(buf, 0, lrecl))
    rowsWritten += 1
  }

  override def close(): Unit = {
    logger.info(s"Closing GcsFileExport for uri:$gcsOutUri  after writing ${rowsWritten * lrecl} bytes and $rowsWritten rows.")
    writer.close()
  }

  override def toString: String = s"GcsFileExport with uri: $gcsOutUri"
}
