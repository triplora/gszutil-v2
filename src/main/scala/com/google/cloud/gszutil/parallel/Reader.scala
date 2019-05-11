package com.google.cloud.gszutil.parallel

import java.nio.ByteBuffer

import akka.actor.{Actor, Props}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.gszutil.parallel.ActorSystem._
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Reader {
  def props(batchSize: Int, in: ZRecordReaderT): Props =
    Props(classOf[Reader], batchSize, in)
}
class Reader(batchSize: Int, in: ZRecordReaderT) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private var batchId = 0
  private var sendTime = -1L
  private var recvTime = -1L
  private val pool = mutable.Stack[Array[Byte]]()
  private val bufSize = in.lRecl * batchSize
  private val maxIOWaitMillis = 42L
  private val maxLog = 42L
  private var nLog = 0L

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
    if (buf.length % in.lRecl != 0) {
      if (nLog < maxLog){
        log.error(s"Buffer ${buf.length} is not a multiple of LRECL")
        nLog += 1
      }
    }
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
    if (bb.position % in.lRecl != 0) {
      if (nLog < maxLog) {
        log.error("read was not a multiple of record length")
        nLog += 1
      }
    }
    Batch(buf, bb.position, batchId, in.lRecl, in.blkSize)
  }

  def sendBatch(batch: Batch): Unit = {
    recvTime = System.currentTimeMillis
    if (recvTime - sendTime > maxIOWaitMillis)
      context.parent ! Available

    if (batch.limit > 0 && batch.limit % in.lRecl == 0) {
      sender ! batch
      batchId += 1
      sendTime = System.currentTimeMillis
    } else {
      if (nLog < maxLog) {
        log.error(s"discarded batch with length ${batch.buf.length} and limit ${batch.limit} % ${in.lRecl} != 0")
        nLog += 1
      }
    }
    if (!in.isOpen) {
      context.parent ! Finished
      context.stop(self)
    }
  }

  def getBuffer(): Array[Byte] =
    if (pool.nonEmpty) pool.pop()
    else new Array[Byte](bufSize)
}
