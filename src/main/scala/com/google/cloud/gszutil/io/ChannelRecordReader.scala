package com.google.cloud.gszutil.io

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class ChannelRecordReader(rc: ReadableByteChannel, recordLength: Int, blockSize: Int) extends ZRecordReaderT {

  private var a: Array[Byte] = _
  private var b: ByteBuffer = _

  override def read(buf: Array[Byte]): Int = {
    if (a != null && buf.equals(a)) {
      b.clear()
    } else {
      a = buf
      b = ByteBuffer.wrap(a)
    }
    rc.read(b)
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (a == null || !buf.equals(a)) {
      a = buf
      b = ByteBuffer.wrap(a)
    }
    b.position(off)
    b.limit(off + len)
    rc.read(b)
  }

  override def close(): Unit = rc.close()

  override def isOpen: Boolean = rc.isOpen

  override val lRecl: Int = recordLength

  override val blkSize: Int = blockSize
}
