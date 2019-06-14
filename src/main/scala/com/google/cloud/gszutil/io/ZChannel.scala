package com.google.cloud.gszutil.io

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class ZChannel(private val reader: ZRecordReaderT) extends ReadableByteChannel {
  private var hasRemaining = true
  private var open = true
  private val data: Array[Byte] = new Array[Byte](reader.blkSize)
  private val buf: ByteBuffer = ByteBuffer.wrap(data)
  private var bytesRead: Long = 0

  buf.position(buf.capacity) // initial buffer state = empty

  override def read(dst: ByteBuffer): Int = {
    if (buf.remaining < dst.capacity && hasRemaining){
      buf.compact()
      val n = reader.read(data, buf.position, buf.remaining)
      if (n > 0) {
        buf.position(buf.position + n)
        bytesRead += n
      } else if (n < 0){
        reader.close()
        hasRemaining = false
      }
      buf.flip()
    }
    val n = math.min(buf.remaining, dst.remaining)
    dst.put(data, buf.position, n)
    buf.position(buf.position + n)
    if (hasRemaining || n > 0) {
      n
    } else {
      close()
      -1
    }
  }

  override def isOpen: Boolean = open
  override def close(): Unit = {
    if (isOpen) {
      reader.close()
      open = false
    }
  }
  def getBytesRead: Long = bytesRead
}
