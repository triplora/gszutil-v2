package com.google.cloud.gszutil

import java.nio.ByteBuffer

import com.google.cloud.gszutil.ZReader.TRecordReader

class TestRecordReader(srcBytes: Array[Byte], lRecl: Int, blkSize: Int) extends TRecordReader {
  private val buf = ByteBuffer.wrap(srcBytes)
  private var open = true

  override def read(bytes: Array[Byte]): Int =
    read(bytes, 0, bytes.length)

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    if (buf.hasRemaining){
      val n = math.min(buf.remaining, len)
      buf.get(bytes, off, n)
      n
    } else -1
  }

  override def close(): Unit = open = false
  override def getLrecl: Int = lRecl
  override def getBlksize: Int = blkSize
}
