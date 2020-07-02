package com.google.cloud.imf.grecv.server

import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.zip.GZIPInputStream

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.CloudLogging.CloudLogger
import com.google.cloud.imf.util.{CloudLogging, Logging}
import com.google.cloud.storage.{BlobId, Storage}
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

object GRecvServerListener extends Logging {
  private val cloudLogger: CloudLogger = CloudLogging.getLogger(this.getClass)

  def write(req: GRecvRequest,
            gcs: Storage,
            id: String,
            responseObserver: StreamObserver[GRecvResponse]) {
    val hasher = Hashing.murmur3_128().newHasher()
    val buf: ByteBuffer = ByteBuffer.allocate(req.getLrecl*1024)
    val orc: WriterCore = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      gcs = gcs,
      maxErrorPct = req.getMaxErrPct,
      name = s"$id",
      lrecl = req.getLrecl)
    var errCount: Long = 0
    var rowCount: Long = 0
    var status: Int = GRecvProtocol.OK
    val jobInfo: java.util.Map[String,Any] = Util.toMap(req.getJobinfo)
    cloudLogger.log("received request\n" + JsonFormat.printer.print(req), jobInfo, CloudLogging.Info)

    val gcsUri = new URI(req.getSrcUri)
    val blob = gcs.get(BlobId.of(gcsUri.getAuthority, gcsUri.getPath.stripPrefix("/")))
    val input = Channels.newChannel(new GZIPInputStream(Channels.newInputStream(blob.reader()),32*1024))

    var n = 0
    while (n > 0) {
      buf.clear()
      n = input.read(buf)
      hasher.putBytes(buf.array(),0,buf.position())
      buf.flip()
      val result = orc.write(buf)
      logger.debug("wrote data to ORC")
      errCount += result.errCount
      rowCount += result.rowCount

      val errPct = errCount.doubleValue() / math.max(1,rowCount)
      if (errPct > req.getMaxErrPct) {
        logger.debug(s"errPct $errPct")
        status = GRecvProtocol.ERR
      }
    }

    val response = GRecvResponse.newBuilder
      .setStatus(status)
      .setHash(hasher.hash().toString)
      .setErrCount(errCount)
      .setRowCount(rowCount)
      .build
    val json = JsonFormat.printer().omittingInsignificantWhitespace().print(response)
    val msg = s"request completed $json"
    logger.info(msg)
    cloudLogger.log(msg, jobInfo, CloudLogging.Info)
    orc.close()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
