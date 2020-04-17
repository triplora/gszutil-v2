package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.io.Bytes
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path

class WriterCore(schemaProvider: SchemaProvider,
                 lrecl: Int,
                 basePath: Path,
                 gcs: Storage,
                 maxErrorPct: Double,
                 name: String) extends Logging {
  val orc = new OrcContext(gcs, schemaProvider.ORCSchema, basePath, name)
  val BatchSize = 1024
  private val reader = new ZReader(schemaProvider, BatchSize, lrecl)
  private val errBuf = ByteBuffer.allocate(lrecl * BatchSize)
  private var errorCount: Long = 0
  private var bytesIn: Long = 0
  def getBytesIn: Long = bytesIn
  def getErrorCount: Long = errorCount

  def write(buf: ByteBuffer): WriteResult = {
    bytesIn += buf.limit()
    val res = orc.write(reader, buf, errBuf)
    errorCount += res.errCount
    if (errBuf.position() > 0){
      errBuf.flip()
      val a = new Array[Byte](schemaProvider.LRECL)
      errBuf.get(a)
      System.err.println(s"Failed to read row")
      System.err.println(Bytes.hexValue(a))
    }
    res
  }

  def close(): Unit = {
    logger.debug("closing ORC Context")
    orc.close()
  }
}
