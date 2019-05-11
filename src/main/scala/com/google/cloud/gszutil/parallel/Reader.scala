package com.google.cloud.gszutil.parallel

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, Props}
import com.google.cloud.gszutil.ZOS
import com.google.cloud.gszutil.io.TRecordReader
import com.google.cloud.gszutil.parallel.ActorSystem.{Available, Batch, Finished}
import org.slf4j.LoggerFactory

object Reader {
  def props(master: ActorRef, dd: String, batchSize: Int): Props =
    Props(classOf[Reader], master, dd, batchSize)
}
class Reader(master: ActorRef, dd: String, batchSize: Int) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private lazy val in: TRecordReader = ZOS.readDD(dd)
  private var batchId: Int = 0

  override def receive: Receive = {
    case Available if in.isOpen =>
      val buf = new Array[Byte](in.lRecl * batchSize)
      sendBatch(read(buf))

    case Batch(buf, _, _) if in.isOpen =>
      sendBatch(read(buf))

    case x =>
      log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
  }

  def read(buf: Array[Byte]): Batch = {
    if (buf.length % in.lRecl != 0)
      log.error(s"Buffer ${buf.length} is not a multiple of LRECL")
    val bb = ByteBuffer.wrap(buf)
    bb.position(0)
    val newLimit = bb.capacity - (bb.capacity % in.lRecl)
    bb.limit(newLimit)
    while (bb.hasRemaining && in.isOpen){
      val n = in.read(buf, bb.position, bb.remaining)
      if (n < 0) {
        in.close()
      } else {
        val newPosition = bb.position + n
        bb.position(newPosition)
      }
    }
    if (bb.position % in.lRecl != 0)
      log.error("read was not a multiple of record length")
    Batch(buf, bb.position, batchId)
  }

  def sendBatch(b: Batch): Unit = {
    if (b.limit > 0) {
      sender() ! b
      batchId += 1
      log.warn("discarded batch with 0 length")
    }
    if (!in.isOpen) {
      master ! Finished
      context.stop(self)
    }
  }
}
