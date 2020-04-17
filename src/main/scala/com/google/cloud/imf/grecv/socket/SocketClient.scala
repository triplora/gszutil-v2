package com.google.cloud.imf.grecv.socket

import com.google.cloud.gszutil.io.{SendResult, ZRecordReaderT}
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.grecv.socket.SocketUtil.connectWithRetry
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.Logging
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat

import scala.util.{Failure, Try}

object SocketClient extends Logging {
  def clientInit(host: String,
                 port: Int,
                 tls: Boolean,
                 request: GRecvRequest,
                 blkSz: Int,
                 lrecl: Int,
                 id: String): ClientContext = {
    val socket = clientConnect(host, port, tls = tls)
    sendRequest(request, socket)
    ClientContext(
      socket = socket,
      blkSz = blkSz,
      lRecl = lrecl,
      id = id,
      hasher = Option(Hashing.murmur3_128().newHasher()))
  }

  def clientConnect(host: String, port: Int, tls: Boolean): SocketHolder = {
    logger.info(s"client: connecting to $host:$port")
    val socket = connectWithRetry(host, port, tls = tls)

    logger.info("client: connected")
    SocketHolder(socket)
  }

  /** Opens sockets and sends SchemaProvider
    * blocks until server responds OK
    *
    * @param in ZRecordReaderT
    * @param request GRecvRequest
    * @param nConnections number of sockets to open
    * @param host server hostname or IP Address
    * @param port server port
    * @param tls use TLS (default: true)
    * @return Try[SendResult]
    */
  def clientBegin(in: ZRecordReaderT,
                  request: GRecvRequest,
                  nConnections: Int,
                  host: String,
                  port: Int,
                  tls: Boolean = true): Try[SendResult] = {
    val n = math.max(1,nConnections)
    logger.debug(s"Opening $n connections to $host:$port")
    val sockets: Array[ClientContext] = (0 to n).map{i =>
      logger.debug(s"initializing client $i")
      clientInit(host, port, id = s"$i", tls = tls, request = request,
        blkSz = in.blkSize, lrecl = in.lRecl)
    }.toArray

    var bytesIn = 0L
    var msgCount = 0L
    var socketId = 0
    var bytesRead = 0

    if (in.isOpen)
      logger.info("Starting to send")
    else {
      logger.error("input is not open")
      return Failure(new RuntimeException("input is not open"))
    }

    Try{
      while (in.isOpen && bytesRead >= 0) {
        // Socket round-robin
        socketId += 1
        if (socketId >= sockets.length)
          socketId = 0

        // Read a single block
        bytesRead = sockets(socketId).sendBlock(in)
        if (bytesRead > 0) {
          bytesIn += bytesRead
          msgCount += 1
        } else {
          logger.debug(s"bytesRead = $bytesRead")
        }
      }
      logger.info(s"Input exhausted after $bytesIn bytes $msgCount messages")
      val rc = sockets.map{socket => Try{socket.finishAndClose()}}.count(_.isFailure)
      SendResult(bytesIn, msgCount, rc)
    }
  }

  def sendRequest(request: GRecvRequest, socket: SocketHolder): Unit = {
    val bytes = request.toByteArray
    logger.debug(JsonFormat.printer.print(request))
    logger.info(s"sending ${bytes.length}")
    socket.putIntBuf(bytes.length, flush = false)
    logger.debug(s"sending request")
    socket.putBuf(bytes,0,bytes.length)
    logger.debug("waiting for response")
    val response = socket.readIntBuf

    if (response != GRecvProtocol.OK) {
      logger.error(s"received $response")
      throw new RuntimeException("unexpected response from server")
    } else
      logger.debug(s"received OK")
  }
}
