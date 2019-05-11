package com.google.cloud.gszutil.parallel

import akka.actor.{Actor, ActorRef, Props}
import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.io.{ZDataSet, ZReader}
import com.google.cloud.gszutil.parallel.ActorSystem._
import com.google.cloud.storage.BlobInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcConf, OrcFile}
import org.slf4j.LoggerFactory

object Writer {
  def props(reader: ActorRef,
            blobInfo: BlobInfo,
            maxBytes: Long): Props =
    Props(classOf[Writer], reader, blobInfo, maxBytes)

  def configuration(c: Configuration = new Configuration(false)): Configuration = {
    OrcConf.COMPRESS.setString(c, "none")
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    c
  }
}

class Writer(readerActor: ActorRef,
             blobInfo: BlobInfo,
             maxBytes: Long,
             copyBook: CopyBook) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private var n = 0 // Stop writing when n >= partLength
  private val maxLog = 42L
  private var nLog = 0L
  private val conf = Writer.configuration()

  override def preStart(): Unit = readerActor ! Available

  def receive: Receive = {
    case batch: Batch =>
      log.info(s"Opening WriteChannel to ${getUri(blobInfo)}")

      val path = new Path(s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}")
      val reader = copyBook.reader
      val writerOptions = OrcFile
        .writerOptions(conf)
        .setSchema(copyBook.getOrcSchema)

      val writer = OrcFile.createWriter(path, writerOptions)
      write(reader, writer, batch)
      sender ! Empty(batch.buf)
      context.become(writing(reader, writer))

    case x =>
      if (nLog < maxLog) {
        log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
        nLog += 1
      }
  }

  def writing(reader: ZReader, writer: org.apache.orc.Writer): Receive = {
    case batch: Batch =>
      write(reader, writer, batch)
      if (n >= maxBytes) {
        finish(writer)
        sender ! Free(batch.buf)
      } else
        sender ! Empty(batch.buf)

    case Finished => // Reader has no more bytes
      finish(writer)

    case x =>
      if (nLog < maxLog) {
        log.error(s"Unable to accept ${x.getClass.getSimpleName} message in writing state")
        nLog += 1
      }
  }

  def write(reader: ZReader, writer: org.apache.orc.Writer, batch: Batch): Unit = {
    val rr = new ZDataSet(batch.buf, batch.lRecl, batch.blkSize, limit = batch.limit)
    reader
      .readOrc(rr)
      .filter(_.size > 0)
      .foreach{batch =>
        writer.addRowBatch(batch)
        batch.reset()
      }
    n += batch.limit
  }

  def finish(writer: org.apache.orc.Writer): Unit = {
    writer.close()
    log.info(s"Closed ${getUri(blobInfo)} ($n bytes)")
    context.stop(self)
  }
}
