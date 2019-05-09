package com.google.cloud.gszutil.io

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel


/** Iterator for Binary MVS Data Set
  *
  * @param rc
  * @param recordLength
  */
case class ByteIterator(rc: ReadableByteChannel, recordLength: Int) extends Iterator[Array[Byte]] {
  private val data: Array[Byte] = new Array[Byte](recordLength)
  private val buf: ByteBuffer = ByteBuffer.wrap(data)
  private var totalBytesRead: Long = 0
  override def hasNext: Boolean = rc.isOpen
  override def next(): Array[Byte] = {
    buf.clear()
    val bytesRead = rc.read(buf)
    totalBytesRead += bytesRead
    if (bytesRead == recordLength) data
    else null
  }
}
