package com.google.cloud.imf.grecv.socket

import java.net.{InetSocketAddress, ServerSocket}

import com.google.cloud.imf.util.Logging

import scala.collection.mutable.ArrayBuffer

case class ServerHolder(serverSocket: ServerSocket) extends AutoCloseable with Logging {
  val sockets: ArrayBuffer[SocketHolder] = ArrayBuffer.empty

  override def close(): Unit = {
    logger.debug("closing sockets")
    sockets.foreach(_.close())
    serverSocket.close()
  }

  def awaitConnection(timeout: Int = 10000): SocketHolder = {
    logger.info(s"Waiting for connection")
    val socket = serverSocket.accept()
    socket.setTcpNoDelay(true)
    socket.setSoTimeout(timeout)
    socket.setReuseAddress(true)
    val sh1 = SocketHolder(socket)
    socket.getRemoteSocketAddress match {
      case x: InetSocketAddress =>
        logger.info(s"Client connected from ${x.getHostString}")
      case _ =>
        logger.info(s"Client connected")
    }
    sockets.append(sh1)
    sh1
  }
}
