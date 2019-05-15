package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Util.Logging

import scala.annotation.tailrec

object ZIterator {
  def apply(reader: ZRecordReaderT): (Array[Byte],ZIterator) = {
    val size = reader.blkSize
    val buf = new Array[Byte](size)
    (buf, new ZIterator(reader, buf))
  }
}

class ZIterator(private val reader: ZRecordReaderT, private val data: Array[Byte]) extends Iterator[Int] with Logging {
  require(data.length % reader.lRecl == 0, "buffer must be a multiple of record length")
  private var bytesRead: Long = 0
  private val lrecl = reader.lRecl
  private val blkSize = reader.blkSize
  private val buf = ByteBuffer.wrap(data)
  buf.position(buf.capacity)
  private var hasRemaining = true

  override def hasNext: Boolean = buf.remaining >= lrecl || hasRemaining

  override def next(): Int = {
    if (!buf.hasRemaining) {
      val n = fill()
      if (n <= 0)
        logger.warn(s"fill() returned $n")
    }
    if (buf.remaining >= lrecl) {
      val pos = buf.position
      buf.position(pos + lrecl)
      pos
    } else {
      if (buf.hasRemaining){
        logger.warn(s"discarded ${buf.remaining} bytes")
      }
      logger.warn(s"buffer empty, returning -1")
      -1
    }
  }

  def getBytesRead: Long = bytesRead

  def readData(): Int = {
    if (buf.position == buf.capacity)
      buf.clear
    else if (buf.limit == buf.capacity)
      buf.compact

    var n = 0
    while (buf.hasRemaining && hasRemaining) {
      val len = math.min(blkSize, buf.remaining)
      val off = buf.position
      val nBytesRead = reader.read(data, off, len)
      if (nBytesRead > 0) {
        val newPos = buf.position + nBytesRead
        n += nBytesRead
        buf.position(newPos)
      } else {
        buf.limit(buf.position)
      }
      if (nBytesRead == -1) {
        hasRemaining = false
      }
    }
    buf.flip()
    n
  }

  def close(): Unit = reader.close()

  @tailrec
  private def fill(tries: Int = 0, limit: Int = 8, wait: Long = 100): Int = {
    if (!hasRemaining) {
      logger.warn("attempted to fill with no data remaining")
      return -1
    }
    val n = readData()
    if (n > 0) {
      bytesRead += n
      n
    } else if (n == -1) {
      logger.info("reader returned -1")
      close()
      -1
    } else if (tries > limit) {
      logger.error("retry limit reached")
      close()
      -1
    } else {
      if (wait > 0){
        logger.warn(s"waiting $wait ms for input")
        Thread.sleep(wait)
      }
      fill(tries = tries + 1, wait = wait * 2)
    }
  }
}