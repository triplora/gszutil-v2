package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, SimpleGCSFileSystem}
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.impl.WriterImpl
import org.apache.orc.{CompressionKind, InMemoryKeystore, NoOpMemoryManager, OrcConf, OrcFile, TypeDescription, Writer}

/**
  *
  * @param cred OAuth2Credentials
  * @param schema ORC TypeDescription
  * @param basePath GCS URI where parts will be written
  * @param prefix the id of this writer which will be appended to output paths
  */
final class OrcContext(private val cred: OAuth2Credentials, schema: TypeDescription,
                       basePath: Path, prefix: String)
  extends AutoCloseable with Logging {

  private final val BytesBetweenFlush: Long = 32*1024*1024
  private final val OptimalGZipBuffer = 32*1024
  private final val PartSize = 128L*1024*1024

  private val fs = new SimpleGCSFileSystem(cred,
    new FileSystem.Statistics(SimpleGCSFileSystem.Scheme))

  private var partId: Long = -1
  private var bytesSinceLastFlush: Long = 0
  private var bytesRead: Long = 0 // bytes received across all partitions
  private var bytesWritten: Long = 0 // bytes written across all partitions
  private var rowsWritten: Long = 0 // rows written across all partitions
  private var partRowsWritten: Long = 0 // rows written to current partition
  private var errors: Long = 0 // errors across all partitions
  private var writer: Writer = _
  private var currentPath: Path = _

  def errPct: Double = errors.doubleValue / rowsWritten

  next() // Initialize Writer and Path

  private val orcConfig: Configuration = {
    val c = new Configuration(false)
    OrcConf.COMPRESS.setString(c, "ZLIB")
    OrcConf.COMPRESSION_STRATEGY.setString(c, "SPEED")
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
    OrcConf.BUFFER_SIZE.setLong(c, OptimalGZipBuffer)
    OrcConf.ROW_INDEX_STRIDE.setLong(c, 0)
    OrcConf.DIRECT_ENCODING_COLUMNS.setString(c, String.join(",",schema.getFieldNames))
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(c, 0)
    c
  }

  def writerOptions(): WriterOptions = OrcFile
    .writerOptions(orcConfig)
    .setSchema(schema)
    .memory(NoOpMemoryManager)
    .compress(CompressionKind.ZLIB)
    .bufferSize(OptimalGZipBuffer)
    .enforceBufferSize()
    .encrypt("")
    .setKeyProvider(new InMemoryKeystore())
    .fileSystem(fs)

  private def newWriter(): (Path,Writer) = {
    partId += 1
    val newPath = basePath.suffix(s"/$prefix-$partId.orc")
    val writer = OrcFile.createWriter(newPath, writerOptions())
    logger.info(s"Opened Writer for $newPath")
    (newPath, writer)
  }

  override def close(): Unit = {
    if (writer != null){
      closeWriter()
      writer = null
    }
  }

  def closeWriter(): Unit = {
    writer.close()
    bytesWritten += fs.getBytesWritten()
    logger.info(s"Closed ORCWriter $currentPath")
    partRowsWritten = 0
    fs.resetStats()
  }

  def next(): Unit = {
    if (writer != null)
      closeWriter()
    val (p,w) = newWriter()
    writer = w
    currentPath = p
  }

  /**
    *
    * @param reader
    * @param buf
    * @param err
    * @return errCount
    */
  def write(reader: ZReader, buf: ByteBuffer, err: ByteBuffer): WriteResult = {
    bytesRead += buf.limit()
    bytesSinceLastFlush += buf.limit()
    val (rowCount,errorCount) = reader.readOrc(buf, writer, err)
    errors += errorCount
    rowsWritten += rowCount
    partRowsWritten += rowCount
    if (buf.remaining > 0) {
      logger.warn(s"ByteBuffer has ${buf.remaining} bytes remaining.")
    }
    if (bytesSinceLastFlush > BytesBetweenFlush) {
      flush()
    }
    val currentPartitionSize = fs.getBytesWritten()
    val partFinished = currentPartitionSize > PartSize
    val partPath = currentPath
    if (partFinished) {
      logger.info(s"Current partition size $currentPartitionSize exceeds target " +
        s"(rows part: $partRowsWritten total: $rowsWritten)")
      next()
    }
    WriteResult(rowCount, errorCount, currentPartitionSize, partFinished, partPath.toString)
  }

  def flush(): Unit = {
    writer match {
      case w: WriterImpl =>
        w.checkMemory(0)
        bytesSinceLastFlush = 0
      case _ =>
    }
  }

  def getCurrentPartRowsWritten: Long = partRowsWritten
  def getCurrentPartBytesWritten: Long = fs.getBytesWritten()
  def getBytesRead: Long = bytesRead
  def getBytesWritten: Long = bytesWritten + fs.getBytesWritten()
  def getRowsWritten: Long = rowsWritten
}
