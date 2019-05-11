package com.google.cloud.gszutil.parallel

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, Props}
import com.google.cloud.WriteChannel
import com.google.cloud.gszutil.parallel.ActorSystem._
import com.google.cloud.storage.{BlobInfo, Storage}
import org.slf4j.LoggerFactory

object Writer {
  def props(parent: ActorRef,
            reader: ActorRef,
            storage: Storage,
            blobInfo: BlobInfo,
            maxBytes: Long): Props =
    Props(classOf[Writer], parent, reader, storage, blobInfo, maxBytes)
}

class Writer(parent: ActorRef,
             reader: ActorRef,
             storage: Storage,
             blobInfo: BlobInfo,
             maxBytes: Long) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private var n = 0 // Stop writing when n >= partLength

  override def preStart(): Unit = reader ! Available

  def receive: Receive = {
    case batch: Batch =>
      log.info(s"Opening ${getUri(blobInfo)} WriteChannel")
      val out = storage.writer(blobInfo)
      write(out, batch)
      sender ! Empty(batch.buf)
      context.become(writing(out))

    case x =>
      log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
  }

  def writing(out: WriteChannel): Receive = {
    case batch: Batch =>
      write(out, batch)
      if (n >= maxBytes) {
        finish(out)
        sender ! Free(batch.buf)
      } else
        sender ! Empty(batch.buf)

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
    log.info(s"Closed ${getUri(blobInfo)} ($n bytes)")
    context.stop(self)
  }
}
