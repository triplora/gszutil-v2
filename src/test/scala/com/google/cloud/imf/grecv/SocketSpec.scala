package com.google.cloud.imf.grecv

import com.google.cloud.imf.grecv.socket.{SocketClient, SocketUtil}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class SocketSpec extends TCPIPSpec {
  def server(tls: Boolean): Future[Int] = Future[Int]{
    val server = SocketUtil.listen(Host, Port, tls = tls)
    val socket = server.awaitConnection()
    var x = 0
    var total = 0

    x = socket.readIntBuf
    logger.info(s"server: received $x")
    total += x
    socket.putIntBuf(x)
    logger.info(s"server: sent $x")

    try {
      while (x >= 0) {
        x = socket.readIntBuf
        logger.info(s"server: received $x")
        if (x > -1) total += x
      }
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
    }
    socket.putIntBuf(total)
    server.close()
    total
  }

  def client(tls: Boolean): Future[Int] = Future[Int]{
    val socket = SocketClient.clientConnect(Host, Port, tls = tls)
    var total = 0
    socket.putIntBuf(42)
    logger.info(s"client: sent 42")
    total += 42
    logger.info(s"client: flushed GZipOutputStream")

    val response = socket.readIntBuf
    logger.info(s"client: received $response")

    for (x <- 0 until 10){
      socket.putIntBuf(x, flush = false)
      total += x
      logger.info(s"client: sent $x")
    }
    socket.putIntBuf(-1)
    logger.info(s"client: sent -1")
    val serverTotal = socket.readIntBuf
    logger.info(s"client: received $serverTotal (expected $total)")
    socket.close()
    total
  }

  "socket" should "send" in {
    val tls = false
    val result = Await.result(Future.sequence(Seq(client(tls),server(tls))), Duration.Inf)
    assert(result.head == result.last)
  }

  it should "tls" in {
    val tls = true
    val result = Await.result(Future.sequence(Seq(client(tls),server(tls))), Duration.Inf)
    assert(result.head == result.last)
  }
}
