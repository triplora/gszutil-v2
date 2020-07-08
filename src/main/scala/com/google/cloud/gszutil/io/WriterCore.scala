package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.io.Bytes
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path

class WriterCore(schemaProvider: SchemaProvider,
                 lrecl: Int,
                 basePath: Path,
                 cred: OAuth2Credentials,
                 name: String) extends Logging {
  require(schemaProvider.LRECL > 0, "LRECL must be a positive number")
  require(schemaProvider.fieldNames.nonEmpty, "schema must not be empty")
  val orc = new OrcContext(cred, schemaProvider.ORCSchema, basePath, name)
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
    res
  }

  def close(): Unit = {
    logger.debug("closing ORC Context")
    orc.close()
  }
}
