package com.google.cloud.imf.grecv.grpc

import java.nio.ByteBuffer

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, WriteRequest, ZOSJobInfo}
import com.google.cloud.imf.util.CloudLogging.CloudLogger
import com.google.cloud.imf.util.{CloudLogging, Logging, SecurityUtils}
import com.google.cloud.storage.Storage
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

class RequestStream(gcs: Storage,
                    id: String,
                    responseObserver: StreamObserver[GRecvResponse])
  extends StreamObserver[WriteRequest] with Logging {
  private val cloudLogger: CloudLogger = CloudLogging.getLogger(this.getClass)

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
  private var zInfo: ZOSJobInfo = _
  private var jobInfo: java.util.Map[String,Any] = _

  def authenticate(recvRequest: GRecvRequest): Unit = {
    req = recvRequest
    zInfo = req.getJobinfo
    jobInfo = Util.toMap(zInfo)
    val msg = "received request\n" + JsonFormat.printer.print(recvRequest)
    cloudLogger.log(msg, jobInfo, CloudLogging.Info)

    buf = ByteBuffer.allocate(req.getLrecl*1024)
    orc = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      gcs = gcs,
      maxErrorPct = req.getMaxErrPct,
      name = s"$id",
      lrecl = req.getLrecl)

    if (!req.getPublicKey.isEmpty && !req.getSignature.isEmpty) {
      val verified = SecurityUtils.verify(
        SecurityUtils.publicKey(req.getPublicKey.toByteArray),
        req.getSignature.toByteArray,
        req.toBuilder.clearSignature.build.toByteArray)
      val validTimestamp = System.currentTimeMillis - req.getTimestamp < TimeLimit
      if (verified && validTimestamp) {
        principal = req.getPrincipal
        val msg = s"Signature verified (principal=$principal)"
        cloudLogger.log(msg, jobInfo, CloudLogging.Info)
      }
      else {
        if (!validTimestamp) {
          cloudLogger.log(s"invalid timestamp ${req.getTimestamp}",
            jobInfo, CloudLogging.Error)
        }
        if (!verified) {
          cloudLogger.log(s"invalid signature", jobInfo, CloudLogging.Error)
        }
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
    if (msgCount == 1 || msgCount % 10000 == 0)
      logger.debug("received WriteRequest")

    val errPct = errCount.doubleValue() / math.max(1,rowCount)
    if (errPct > req.getMaxErrPct) {
      status = GRecvProtocol.ERR
    }
  }

  override def onError(t: Throwable): Unit =
    cloudLogger.error("error: " + t.getMessage, jobInfo, t)

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
    logger.info(s"request completed $json")
    orc.close()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
    cloudLogger.log(s"request complete $json", jobInfo, CloudLogging.Info)
  }
}
