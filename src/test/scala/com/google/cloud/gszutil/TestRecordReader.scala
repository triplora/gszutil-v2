package com.google.cloud.gszutil

import java.nio.ByteBuffer

import com.google.cloud.gszutil.ZReader.TRecordReader

class TestRecordReader(bytes: Array[Byte], lrecl: Int) extends TRecordReader {
  private val buf: ByteBuffer = ByteBuffer.wrap(bytes)
  private val lreclValue: Int = lrecl

  override def read(bytes: Array[Byte]): Int =
    read(bytes, 0, bytes.length)

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    if (buf.hasRemaining) {
      val nBytes = math.min(buf.remaining, len)
      buf.get(bytes, off, nBytes)
      nBytes
    } else -1
  }

  override def close(): Unit = {}

  override def getLrecl: Int = lreclValue
}
