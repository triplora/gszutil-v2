package com.google.cloud.imf.grecv.socket

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.Socket
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.cloud.imf.util.{Bits, Logging}

import scala.util.Try

case class SocketHolder(socket: Socket) extends AutoCloseable with Logging {
  final val DeflateBufferSize = 4096
  final val BufferSize = 64*1024

  private val os = socket.getOutputStream
  private val is = socket.getInputStream

  private val writer: BufferedOutputStream = new BufferedOutputStream(
      new GZIPOutputStream(os, DeflateBufferSize, true), BufferSize)

  private val reader: BufferedInputStream = new BufferedInputStream(
      new GZIPInputStream(is, DeflateBufferSize), BufferSize)

  def isClosed: Boolean = Try{is.read()}.toOption.contains(-1)

  def readBuf(buf: Array[Byte]): Int = reader.read(buf, 0, buf.length)

  def readIntBuf: Int = Bits.getInt(reader)

  def putIntBuf(x: Int, flush: Boolean = true): Unit = {
    Bits.putInt(writer, x)
    if (flush) writer.flush()
  }

  def putBuf(x: Array[Byte], off: Int, len: Int, flush: Boolean = true): Unit = {
    writer.write(x, off, len)
    if (flush) writer.flush()
  }

  override def close(): Unit = {
    writer.close()
    logger.debug("Closing socket")
    socket.close()
  }
}
