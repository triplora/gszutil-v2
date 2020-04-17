package com.google.cloud.imf.grecv.socket

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

import com.google.cloud.imf.util.Logging
import com.google.common.hash.Hasher

case class ClientContext(socket: SocketHolder,
                         blkSz: Int,
                         lRecl: Int,
                         id: String,
                         hasher: Option[Hasher] = None) extends Logging {
  private val buf = ByteBuffer.allocate(blkSz)
  private var nSent = 0L
  private var bytesSent = 0L
  init()

  private def init(): Unit = {
    logger.debug(s"sending $blkSz")
    socket.putIntBuf(blkSz)
    logger.info(s"$id sent block size $blkSz")
  }

  def send(buf: Array[Byte], off: Int, len: Int): Unit = {
    val n = buf.length
    socket.putIntBuf(n, flush = false)
    socket.putBuf(buf,0,buf.length, flush = false)
    bytesSent += n
    if (hasher.isDefined)
      hasher.get.putBytes(buf)
  }

  def sendBlock(in: ReadableByteChannel): Int = {
    var n = 0
    buf.clear
    while (buf.remaining >= lRecl && n > -1)
      n = in.read(buf)

    if (buf.position() > 0) {
      send(buf.array,0,buf.position())
      nSent += 1
    }
    n
  }

  def finishAndClose(): Unit = {
    logger.info(s"$id sending end of stream")
    socket.putIntBuf(-1)
    logger.debug(s"$id waiting for confirmation")
    val received = socket.readIntBuf
    logger.info(s"$id received confirmation of $received blocks")
    if (socket.isClosed)
      logger.info(s"$id server closed socket")
    close()
    if (received != nSent)
      throw new RuntimeException(s"received $received != sent $nSent")
  }

  def close(): Unit = {
    socket.close()
    logger.debug(s"$id writer closed")
    result.foreach{hash => logger.info(s"$id hash=$hash")}
  }

  def result: Option[String] = hasher.map(_.hash.toString)
}
