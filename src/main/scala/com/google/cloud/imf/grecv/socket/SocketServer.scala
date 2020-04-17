package com.google.cloud.imf.grecv.socket

import java.nio.ByteBuffer

import com.google.cloud.bqsh.GCS
import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.{WriteResult, WriterCore}
import com.google.cloud.imf.grecv.GRecvProtocol.{ERR, OK}
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.cloud.storage.Storage
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SocketServer extends Logging {
  def serverBegin(server: ServerHolder)
                 (implicit ec: ExecutionContext): Unit = {
    var i = 0
    while (true){
      val conn = Await.result(Future{Try(server.awaitConnection())}, Duration.Inf)
      conn match {
        case Failure(e) =>
          if (e.getMessage.contains("Accept timed out"))
            logger.debug("Accept timed out")
          else logger.error(e.getMessage, e)
        case Success(socket) =>
          i += 1
          Future{
            val id = s"$i"
            serverAccept(socket, GCS.getDefaultClient(), id = id)
          }
      }
    }
  }

  def accept(id: String,
             maxErrorPct: Double,
             core: WriterCore,
             results: ArrayBuffer[WriteResult],
             buf: ByteBuffer): Unit = {
    val res = core.write(buf)
    if (res.partFinished)
      results.append(res)
    if (core.orc.errPct > maxErrorPct)
      throw new RuntimeException(s"$id errPct ${core.orc.errPct} > $maxErrorPct")
    else if (res.partFinished)
      logger.info(s"$id part finished: ${res.partPath} ${res.partBytes} bytes")
  }

  def serverAccept(socket: SocketHolder, gcs: Storage, id: String): Array[WriteResult] = {
    val request = receiveRequest(socket)
    val schemaProvider = RecordSchema(request.getSchema)
    val maxErrorPct = request.getMaxErrPct

    val ctx = ServerContext(socket, id = id, hasher = Option(Hashing.murmur3_128().newHasher()))

    val core: WriterCore = new WriterCore(schemaProvider = schemaProvider,
      basePath = new Path(request.getBasepath),
      gcs = gcs,
      maxErrorPct = maxErrorPct,
      name = id,
      lrecl = request.getLrecl)

    val results: ArrayBuffer[WriteResult] = ArrayBuffer.empty

    ctx.read(accept(id, maxErrorPct, core, results, _))
    ctx.close()
    core.orc.close()
    val recordsIn = core.getBytesIn / schemaProvider.LRECL
    val errorPct = core.getErrorCount.toDouble / recordsIn
    if (errorPct > maxErrorPct) {
      val msg = s"$id error percent $errorPct exceeds threshold of $maxErrorPct"
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    results.toArray
  }

  def receiveRequest(socket: SocketHolder): GRecvRequest = {
    logger.debug("receiving schema size")
    val n = socket.readIntBuf
    logger.debug(s"receiving request with size $n")
    val bytes = new Array[Byte](n)
    socket.readBuf(bytes)
    logger.debug(s"receiving request with size $n")
    try {
      val request = GRecvRequest.parseFrom(bytes)
      logger.info(s"received request:\n" +
        JsonFormat.printer.omittingInsignificantWhitespace.print(request))

      if (!request.getSignature.isEmpty && !request.getPublicKey.isEmpty){
        logger.debug(s"checking signature")
        logger.info(s"principal = ${request.getPrincipal}")
        val publicKey = SecurityUtils.publicKey(request.getPublicKey.toByteArray)
        val publicKeyHash = SecurityUtils.hashKey(publicKey)
        logger.info(s"public key SHA256 = $publicKeyHash")
        val signatureVerified = SecurityUtils.verify(
          publicKey,
          request.getSignature.toByteArray,
          request.toBuilder.clearSignature.build.toByteArray)
        if (!signatureVerified)
          logger.warn("unable to verify signature")
        else
          logger.info("signature verified")
      } else {
        logger.info("signature and public key not ")
      }

      socket.putIntBuf(OK)
      logger.debug(s"sent OK")
      request
    } catch {
      case e: Throwable =>
        socket.putIntBuf(ERR)
        logger.error(s"sent ERR; ${e.getMessage}", e)
        throw e
    }
  }
}
