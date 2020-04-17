package com.google.cloud.imf.grecv.socket

import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.channels.SocketChannel
import java.nio.file.{Files, Paths}

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.{Logging, TLSUtil}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object SocketUtil extends Logging {
  private val ConnQueue = 5

  def listen(host: String, port: Int, tls: Boolean, timeout: Int = 90000,
             chain: String = "", key: String = ""): ServerHolder = {
    val addr = InetAddress.getByName(host)
    logger.info(s"server: binding $host:$port tls:$tls")
    val serverSocket =
      if (tls) {
        val hasChain = Files.isRegularFile(Paths.get(chain))
        val hasKey = Files.isRegularFile(Paths.get(key))
        logger.debug(s"chain: '$chain' $hasChain key: '$key' $hasKey")
        if (chain.nonEmpty && key.nonEmpty && hasChain && hasKey)
          TLSUtil.tlsSocket(chain, key, host, port, ConnQueue)
        else
          TLSUtil.tlsSocket(host, port, ConnQueue)
      }
      else new ServerSocket(port, ConnQueue, addr)
    ServerHolder(serverSocket)
  }

  @tailrec
  def connectWithRetry(host: String,
                       port: Int,
                       retries: Int = 20,
                       wait: Long = 100,
                       tls: Boolean): Socket = {
    Try{
      val sc = SocketChannel.open(new InetSocketAddress(host, port))
      if (tls) {
        logger.debug("Connecting with TLS")
        //TLSUtil.getDefaultSSLSocketFactory.createSocket(host, port)
      } else {
        logger.warn("Connecting with plaintext")
        //val socket = SocketChannel.open().socket()
        //socket.connect(new InetSocketAddress(host, port))
        //socket
      }
      sc
    } match {
      case Success(socket) =>
        socket.socket()
      case Failure(e) =>
        if (retries < 0)
          throw e
        else {
          Util.sleepOrYield(wait)
          connectWithRetry(host, port, retries - 1, wait * 2, tls = tls)
        }
    }
  }
}
