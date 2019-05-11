package com.google.cloud.gszutil.parallel

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, Props}
import com.google.cloud.gszutil.ZOS
import com.google.cloud.gszutil.parallel.ActorSystem.{Available, Batch, Empty, Finished, Free}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Reader {
  def props(master: ActorRef, dd: String, batchSize: Int): Props =
    Props(classOf[Reader], master, dd, batchSize)
}
class Reader(master: ActorRef, dd: String, batchSize: Int) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private lazy val in = ZOS.readDD(dd)
  private var batchId = 0
  private val pool = mutable.Stack[Array[Byte]]()
  private val bufSize = in.lRecl * batchSize

  override def receive: Receive = {
    case Available if in.isOpen =>
      sendBatch(read(getBuffer()))

    case Empty(buf) if in.isOpen && buf.length == bufSize =>
      sendBatch(read(buf))

    case Free(buf) if buf.length == bufSize =>
      pool.push(buf)

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

  def sendBatch(batch: Batch): Unit = {
    if (batch.limit > 0 && batch.limit % in.lRecl == 0) {
      sender ! batch
      batchId += 1
    } else
      log.error(s"discarded batch with length ${batch.buf.length} and limit ${batch.limit} % ${in.lRecl} != 0")
    if (!in.isOpen) {
      master ! Finished
      context.stop(self)
    }
  }

  def getBuffer(): Array[Byte] =
    if (pool.nonEmpty) pool.pop()
    else new Array[Byte](bufSize)
}
