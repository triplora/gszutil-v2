package com.google.cloud.gszutil.orc

import java.nio.ByteBuffer

import akka.actor.Actor
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZReader
import com.google.cloud.pso.SimpleORCWriter
import org.apache.hadoop.fs.{FileSystem, SimpleGCSFileSystem}
import org.apache.orc.impl.WriterImpl
import org.apache.orc.{CompressionKind, NoOpMemoryManager, OrcFile, Writer}

/** Responsible for writing a single output partition
  */
class ORCFileWriter(args: ORCFileWriterArgs) extends Actor with Logging {
  import args._
  private val reader = new ZReader(copyBook, batchSize)
  private var bytesIn: Long = 0
  private var bytesSinceLastFlush: Long = 0
  private var elapsedTime: Long = 0
  private var startTime: Long = -1
  private var endTime: Long = -1
  private var writer: Writer = _
  private val stats = new FileSystem.Statistics(SimpleGCSFileSystem.Scheme)

  override def preStart(): Unit = {
    val writerOptions = OrcFile
      .writerOptions(SimpleORCWriter.configuration())
      .setSchema(copyBook.ORCSchema)
      .memory(NoOpMemoryManager)
      .compress(CompressionKind.ZLIB)
      .fileSystem(new SimpleGCSFileSystem(gcs, stats))
    writer = OrcFile.createWriter(path, writerOptions)
    context.parent ! ByteBuffer.allocate(copyBook.LRECL * batchSize)
    startTime = System.currentTimeMillis()
    logger.info(s"Starting writer for ${args.path} ${Util.logMem()}")
  }

  override def receive: Receive = {
    case x: ByteBuffer =>
      bytesIn += x.limit
      bytesSinceLastFlush += x.limit
      val t0 = System.currentTimeMillis
      reader.readOrc(x, writer)
      if (bytesSinceLastFlush > 32L * 1024L * 1024L) {
        writer match {
          case w: WriterImpl =>
            w.checkMemory(1.0d)
            bytesSinceLastFlush = 0
          case _ =>
        }
      }
      val t1 = System.currentTimeMillis
      elapsedTime += (t1 - t0)
      val partBytesRemaining = maxBytes - stats.getBytesWritten
      if (partBytesRemaining > 0) {
        sender ! x
      } else {
        context.stop(self)
      }
    case _ =>
  }

  override def postStop(): Unit = {
    val t0 = System.currentTimeMillis
    writer.close()
    val t1 = System.currentTimeMillis
    elapsedTime += (t1 - t0)
    endTime = System.currentTimeMillis
    val dt = endTime - startTime
    val idle = dt - elapsedTime
    val bytesOut = stats.getBytesWritten
    val ratio = (bytesOut * 1.0d) / bytesIn
    val mbps = Util.fmbps(bytesOut, elapsedTime)
    logger.info(s"Stopping writer for ${args.path} after writing $bytesOut bytes in $elapsedTime ms ($mbps mbps) $dt ms total $idle ms idle $bytesIn bytes read ${f"$ratio%1.2f"} compression ratio ${Util.logMem()}")
  }
}
