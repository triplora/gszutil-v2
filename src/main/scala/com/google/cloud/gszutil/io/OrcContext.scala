package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import akka.io.BufferPool
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.Storage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, SimpleGCSFileSystem}
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.impl.WriterImpl
import org.apache.orc.{CompressionKind, NoOpMemoryManager, OrcConf, OrcFile, TypeDescription, Writer}

final class OrcContext(private val gcs: Storage, schema: TypeDescription, compress: Boolean,
                 path: Path, prefix: String, maxBytes: Long, pool: BufferPool)
  extends AutoCloseable with Logging {

  private val BytesBetweenFlush: Long = 32*1024*1024
  private val OptimalGZipBuffer = 32*1024

  private val fs = new SimpleGCSFileSystem(gcs,
    new FileSystem.Statistics(SimpleGCSFileSystem.Scheme))
  private val timer = new WriteTimer

  private var partId: Long = -1
  private var bytesSinceLastFlush: Long = 0
  private var bytesIn: Long = 0
  private var bytesOut: Long = 0
  private var writer: Writer = _
  private var currentPath: Path = _

  next() // Initialize Writer and Path

  private final val orcConfig = {
    val c = new Configuration(false)
    if (compress){
      OrcConf.COMPRESS.setString(c, "ZLIB")
      OrcConf.COMPRESSION_STRATEGY.setString(c, "SPEED")
    } else {
      OrcConf.COMPRESS.setString(c, "NONE")
    }
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
    OrcConf.BUFFER_SIZE.setLong(c, OptimalGZipBuffer)
    val columns = String.join(",",schema.getFieldNames)
    OrcConf.DIRECT_ENCODING_COLUMNS.setString(c, columns)
    c
  }

  def writerOptions(): WriterOptions = OrcFile
    .writerOptions(orcConfig)
    .setSchema(schema)
    .memory(NoOpMemoryManager)
    .compress(if (compress) CompressionKind.ZLIB else CompressionKind.NONE)
    .bufferSize(OptimalGZipBuffer)
    .enforceBufferSize()
    .fileSystem(fs)

  private def newWriter(): (Path,Writer) = {
    partId += 1
    val newPath = path.suffix(s"$prefix-$partId.orc")
    val writer = OrcFile.createWriter(newPath, writerOptions())
    (newPath, writer)
  }

  override def close(): Unit = {
    if (writer != null){
      timer.start()
      writer.close()
      timer.end()
      bytesOut + fs.getBytesWritten()
      fs.resetStats()
      timer.close(logger, s"Stopping writer for $currentPath", bytesIn, getBytesWritten)
      writer = null
    }
  }

  def next(): Unit = {
    if (writer != null) {
      writer.close()
      bytesOut += fs.getBytesWritten()
    }
    fs.resetStats()
    timer.reset()
    val (p,w) = newWriter()
    writer = w
    currentPath = p
  }

  def write(reader: ZReader, buf: ByteBuffer, err: ByteBuffer): Long = {
    bytesIn += buf.limit
    bytesSinceLastFlush += buf.limit
    timer.start()
    val errorCount = reader.readOrc(buf, writer, err)
    timer.end()
    if (bytesSinceLastFlush > BytesBetweenFlush) {
      timer.start()
      flush()
      timer.end()
    }
    if (maxBytes - fs.getBytesWritten() < 1) next()
    pool.release(buf)
    errorCount
  }

  def flush(): Unit = {
    writer match {
      case w: WriterImpl =>
        timer.start()
        w.checkMemory(1.0d)
        timer.end()
        bytesSinceLastFlush = 0
      case _ =>
    }
  }

  def getBytesWritten: Long = bytesOut + fs.getBytesWritten()
}
