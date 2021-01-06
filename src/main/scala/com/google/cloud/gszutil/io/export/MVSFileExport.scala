package com.google.cloud.gszutil.io.`export`

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Transcoder
import com.google.cloud.gszutil.io.ZRecordWriterT
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.Logging

case class MVSFileExport(outDD: String, zos: MVS) extends FileExport with Logging {
  val ddName: String = outDD
  logger.debug(s"Initializing writer for DD:$ddName")
  val writer: ZRecordWriterT = zos.writeDD(ddName)
  val dsn: String = writer.getDsn
  logger.debug(s"Opened DSN: $dsn")
  val lRecl: Int = writer.lRecl
  override val transcoder: Transcoder = zos.transcoder
  private val buf = ByteBuffer.allocate(lRecl)
  private var bytesWritten = 0L

  override def appendBytes(bytes: Array[Byte]): Unit = {
    buf.clear()
    buf.put(bytes)
    buf.flip()
    writer.write(buf)
    bytesWritten += bytes.length
  }

  override def close(): Unit = {
    logger.info(s"Closing RecordWriter for DD:$ddName DSN:$dsn after writing $bytesWritten bytes")
    writer.flush()
    writer.close()
  }

  override def rowsWritten(): Long = writer.count()
}
