package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import akka.io.BufferPool
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.Storage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, SimpleGCSFileSystem}
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.impl.WriterImpl
import org.apache.orc.{CompressionKind, InMemoryKeystore, NoOpMemoryManager, OrcConf, OrcFile, TypeDescription, Writer}

/**
  *
  * @param gcs GCS client
  * @param schema ORC TypeDescription
  * @param basePath GCS URI where parts will be written
  * @param prefix the id of this writer which will be appended to output paths
  * @param maxBytes part size
  * @param pool BufferPool
  */
final class OrcContext(private val gcs: Storage, schema: TypeDescription,
                       basePath: Path, prefix: String, maxBytes: Long, pool: BufferPool)
  extends AutoCloseable with Logging {

  private val BytesBetweenFlush: Long = 32*1024*1024
  private val OptimalGZipBuffer = 32*1024

  private val fs = new SimpleGCSFileSystem(gcs,
    new FileSystem.Statistics(SimpleGCSFileSystem.Scheme))

  private var partId: Long = -1
  private var bytesSinceLastFlush: Long = 0
  private var bytesIn: Long = 0
  private var bytesOut: Long = 0
  private var rowsOut: Long = 0
  private var rowsOutPart: Long = 0
  private var errors: Long = 0
  private var writer: Writer = _
  private var currentPath: Path = _

  def errPct: Double = errors.doubleValue / rowsOut

  next() // Initialize Writer and Path

  private val orcConfig: Configuration = {
    val c = new Configuration(false)
    OrcConf.COMPRESS.setString(c, "ZLIB")
    OrcConf.COMPRESSION_STRATEGY.setString(c, "SPEED")
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
    OrcConf.BUFFER_SIZE.setLong(c, OptimalGZipBuffer)
    OrcConf.DIRECT_ENCODING_COLUMNS.setString(c, String.join(",",schema.getFieldNames))
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
      writer.close()
      bytesOut += fs.getBytesWritten()
      fs.resetStats()
      writer = null
    }
  }

  def next(): Unit = {
    if (writer != null) {
      writer.close()
      logger.info(s"Closed Writer for $currentPath")
      bytesOut += fs.getBytesWritten()
    }
    rowsOutPart = 0
    fs.resetStats()
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
    bytesIn += buf.limit
    bytesSinceLastFlush += buf.limit
    val (rowCount,errorCount) = reader.readOrc(buf, writer, err)
    errors += errorCount
    rowsOut += rowCount
    rowsOutPart += rowCount
    if (buf.remaining > 0) {
      logger.warn(s"ByteBuffer has ${buf.remaining} bytes remaining.")
    }
    if (bytesSinceLastFlush > BytesBetweenFlush) {
      flush()
    }
    val currentPartitionSize = fs.getBytesWritten()
    val partFinished = currentPartitionSize > maxBytes
    val partPath = currentPath
    if (partFinished) {
      logger.info(s"Current partition size $currentPartitionSize exceeds target " +
        s"(rows part: $rowsOutPart total: $rowsOut)")
      next()
    }
    pool.release(buf)
    WriteResult(rowCount, errorCount, currentPartitionSize, partFinished, partPath.toString)
  }

  def flush(): Unit = {
    writer match {
      case w: WriterImpl =>
        w.checkMemory(1.0d)
        bytesSinceLastFlush = 0
      case _ =>
    }
  }

  def currentPartRows: Long = rowsOutPart
  def totalRows: Long = rowsOut
  def currentPartSize: Long = fs.getBytesWritten()
  def getBytesWritten: Long = bytesOut + fs.getBytesWritten()
}
