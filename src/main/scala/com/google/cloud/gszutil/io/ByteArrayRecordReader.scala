package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Util.Logging

class ByteArrayRecordReader(srcBytes: Array[Byte], recordLength: Int, blockSize: Int) extends TRecordReader with Logging {
  private val buf = ByteBuffer.wrap(srcBytes)
  private var open = true
  private var bytesRead: Long = 0

  override def read(bytes: Array[Byte]): Int =
    read(bytes, 0, bytes.length)

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    if (buf.hasRemaining){
      val n = math.min(buf.remaining, len)
      buf.get(bytes, off, n)
      bytesRead += n
      n
    } else -1
  }

  override def close(): Unit = {
    logger.info(s"close() read $bytesRead bytes")
    open = false
  }
  override val lRecl: Int = recordLength
  override val blkSize: Int = blockSize
}
