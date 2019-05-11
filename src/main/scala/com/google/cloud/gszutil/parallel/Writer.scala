package com.google.cloud.gszutil.parallel

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, Props}
import com.google.cloud.WriteChannel
import com.google.cloud.gszutil.parallel.ActorSystem.{Available, Batch, Finished, getUri}
import com.google.cloud.storage.{BlobInfo, Storage}
import org.slf4j.LoggerFactory

object Writer {
  def props(parent: ActorRef,
            reader: ActorRef,
            storage: Storage,
            blobInfo: BlobInfo,
            partLength: Long): Props =
    Props(classOf[Writer], parent, reader, storage, blobInfo, partLength)
}

class Writer(parent: ActorRef,
             reader: ActorRef,
             storage: Storage,
             blobInfo: BlobInfo,
             partLength: Long) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private var n = 0 // Stop writing when n >= partLength

  override def preStart(): Unit = {
    super.preStart()
    reader ! Available
  }

  def receive: Receive = {
    case batch: Batch =>
      val out = storage.writer(blobInfo)
      log.info(s"Started writing to ${getUri(blobInfo)}")
      write(out, batch)
      sender() ! batch
      context.become(writing(out))

    case x =>
      log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
  }

  def writing(out: WriteChannel): Receive = {
    case batch: Batch =>
      write(out, batch)
      if (n < partLength)
        sender() ! Available
      else
        finish(out)

    case Finished => // Reader has no more bytes
      finish(out)

    case x =>
      log.error(s"Unable to accept ${x.getClass.getSimpleName} message in writing state")
  }

  def write(out: WriteChannel, batch: Batch): Unit = {
    val buf = ByteBuffer.wrap(batch.buf)
    buf.position(0)
    buf.limit(batch.limit)
    while (buf.hasRemaining)
      out.write(buf)
    n += batch.limit
  }

  def finish(out: WriteChannel): Unit = {
    out.close()
    log.info(s"${self.path} Finished writing $n bytes to ${getUri(blobInfo)}")
    context.stop(self)
  }
}
