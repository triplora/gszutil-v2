package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

class ZDataSet(srcBytes: Array[Byte], recordLength: Int, blockSize: Int, limit: Int = -1, position: Int = 0) extends ZRecordReaderT {
  private val buf = ByteBuffer.wrap(srcBytes)
  private var open = true
  private var bytesRead: Long = 0
  buf.position(position)
  if (limit >= 0) buf.limit(limit)

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

  override def isOpen: Boolean = open || buf.hasRemaining
  override def close(): Unit = open = false
  override val lRecl: Int = recordLength
  override val blkSize: Int = blockSize
}
