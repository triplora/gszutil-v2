package com.google.cloud.imf.grecv

import java.nio.ByteBuffer

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.imf.grecv.socket.{ServerContext, SocketClient, SocketServer, SocketUtil}
import com.google.common.hash.Hashing

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ProtocolSpec extends TCPIPSpec {
  "grecv" should "protocol" in {
    val client = Future{
      val ctx = SocketClient.clientInit(Host, Port, tls = false, request = request.build,
        blkSz = in.blkSize, lrecl = in.lRecl, "1")

      var bytesRead = 0
      while (in.isOpen && bytesRead > -1){
        bytesRead = ctx.sendBlock(in)
      }
      ctx.finishAndClose()
    }
    val server = Future{
      val server = SocketUtil.listen(Host, Port, tls = false)
      val socket = server.awaitConnection()
      val request = SocketServer.receiveRequest(socket)
      val schemaProvider = RecordSchema(request.getSchema)
      val maxErrorPct = request.getMaxErrPct

      val ctx = ServerContext(socket, id = "s", hasher = Option(Hashing.murmur3_128().newHasher()))
      def accept(buf: ByteBuffer): Unit = {}
      ctx.read(accept)
      ctx.close()
    }

    Await.result(Future.sequence(Seq(client,server)),Duration.Inf)
  }
}
