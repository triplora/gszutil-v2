package com.google.cloud.imf.grecv.socket

import java.nio.ByteBuffer

import com.google.cloud.imf.util.Logging
import com.google.common.hash.Hasher

case class ServerContext(socket: SocketHolder,
                         id: String = "",
                         hasher: Option[Hasher] = None)
  extends Logging {
  private final val blkSz: Int = socket.readIntBuf
  logger.info(s"$id received block size $blkSz")
  private final val buf1: ByteBuffer = ByteBuffer.allocate(blkSz)
  private val LogFrequency = 1000
  private var nRecvd = 0
  private var bytesRecvd = 0L

  def read(f: ByteBuffer => Unit): Unit = {
    logger.info(s"$id starting recv")
    var n = 0
    while (n > -1){
      val len = socket.readIntBuf
      if (len == -1){
        logger.info(s"$id end of stream")
        n = -1
        socket.putIntBuf(nRecvd)
        logger.info(s"$id received $nRecvd blocks $bytesRecvd bytes")
        socket.close()
      } else {
        buf1.clear
        n = socket.readBuf(buf1.array())
        if (n > 0) {
          buf1.position(n)
          buf1.limit(n)
          buf1.flip()
          if (hasher.isDefined)
            hasher.get.putBytes(buf1.array, 0, buf1.limit)
          f.apply(buf1)
          nRecvd += 1
          bytesRecvd += n
          if (nRecvd % LogFrequency == 0)
            logger.debug(s"$id read $nRecvd blocks")
        } else if (n == -1){
          logger.error(s"$id unexpected end of stream")
          socket.close()
        }
      }
    }
  }

  def close(): Unit = {
    socket.close()
    result.foreach{hash => logger.info(s"$id hash=$hash")}
  }

  def result: Option[String] = hasher.map(_.hash().toString)
}
