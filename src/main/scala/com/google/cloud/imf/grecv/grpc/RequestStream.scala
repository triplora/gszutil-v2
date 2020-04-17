package com.google.cloud.imf.grecv.grpc

import java.nio.ByteBuffer

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, WriteRequest}
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.cloud.storage.Storage
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

class RequestStream(gcs: Storage,
                    id: String,
                    responseObserver: StreamObserver[GRecvResponse])
  extends StreamObserver[WriteRequest] with Logging {
  private val hasher = Hashing.murmur3_128().newHasher()
  private var principal: String = _
  private var req: GRecvRequest = _
  private var buf: ByteBuffer = _
  private var orc: WriterCore = _
  private var msgCount: Long = 0
  private var errCount: Long = 0
  private var rowCount: Long = 0
  private var status: Int = GRecvProtocol.OK
  private val TimeLimit: Long = 5L*50*1000

  def authenticate(recvRequest: GRecvRequest): Unit = {
    logger.info("received request\n" + JsonFormat.printer.print(recvRequest))
    req = recvRequest
    buf = ByteBuffer.allocate(req.getLrecl*1024)
    orc = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      gcs = gcs,
      maxErrorPct = req.getMaxErrPct,
      name = s"$id",
      lrecl = req.getLrecl)
    Util.setZInfo(req.getJobinfo)

    if (!req.getPublicKey.isEmpty && !req.getSignature.isEmpty) {
      val verified = SecurityUtils.verify(
        SecurityUtils.publicKey(req.getPublicKey.toByteArray),
        req.getSignature.toByteArray,
        req.toBuilder.clearSignature.build.toByteArray)
      val validTimestamp = System.currentTimeMillis - req.getTimestamp < TimeLimit
      if (verified && validTimestamp) {
        principal = req.getPrincipal
        logger.info(s"Signature verified (principal=$principal)")
      }
      else {
        if (!validTimestamp)
          logger.error(s"invalid timestamp ${req.getTimestamp}")
        if (!verified)
          logger.error(s"invalid signature")
        //throw new StatusRuntimeException(Status.UNAUTHENTICATED)
      }
    }
  }

  override def onNext(value: WriteRequest): Unit = {
    if (req == null)
      authenticate(value.getRequest)

    buf.clear()
    value.getData.copyTo(buf)
    hasher.putBytes(buf.array(),0,buf.position())
    buf.flip()
    val result = orc.write(buf)
    errCount += result.errCount
    rowCount += result.rowCount
    msgCount += 1
    val errPct = errCount.doubleValue() / math.max(1,rowCount)
    if (errPct > req.getMaxErrPct) {
      status = GRecvProtocol.ERR
    }
  }

  override def onError(t: Throwable): Unit = {
    logger.error("error: " + t.getMessage, t)
  }

  def buildResponse(status: Int): GRecvResponse =
    GRecvResponse.newBuilder
      .setStatus(status)
      .setHash(hasher.hash.toString)
      .setErrCount(errCount)
      .setRowCount(rowCount)
      .setMsgCount(msgCount)
      .build

  override def onCompleted(): Unit = {
    val response = buildResponse(status)
    val json = JsonFormat.printer().omittingInsignificantWhitespace().print(response)
    logger.info(s"request complete $json")
    logger.debug("closing orc")
    orc.close()
    logger.debug("orc closed")
    responseObserver.onNext(response)
    responseObserver.onCompleted()
    logger.debug("request completed")
  }
}
