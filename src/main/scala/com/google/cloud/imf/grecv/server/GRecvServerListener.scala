package com.google.cloud.imf.grecv.server

import java.net.URI
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.{Logging, StorageObjectInputStream}
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

object GRecvServerListener extends Logging {
  def write(req: GRecvRequest,
            creds: OAuth2Credentials,
            id: String,
            responseObserver: StreamObserver[GRecvResponse]): Unit = {
    val jobInfo: java.util.Map[String,Any] = Util.toMap(req.getJobinfo)
    val msg1 = "received request\n" + JsonFormat.printer.print(req)
    logger.info(msg1)

    logger.debug(s"opening ${req.getSrcUri}")
    val gcsUri = new URI(req.getSrcUri)
    val bucket = gcsUri.getAuthority
    val name = gcsUri.getPath.stripPrefix("/")
    val is = new StorageObjectInputStream(creds.getAccessToken, bucket, name)

    logger.debug(s"opening GCS blob reader")
    val input = new GZIPInputStream(is,32*1024)

    val hasher = Hashing.murmur3_128().newHasher()
    val buf: ByteBuffer = ByteBuffer.allocate(req.getLrecl*1024)

    logger.debug(s"creating ORC writer $id")
    val orc: WriterCore = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      cred = creds,
      maxErrorPct = req.getMaxErrPct,
      name = s"$id",
      lrecl = req.getLrecl)
    var errCount: Long = 0
    var rowCount: Long = 0
    var bytesRead: Long = 0
    var status: Int = GRecvProtocol.OK

    var n = 0
    while (n >= 0) {
      buf.clear()
      while (n >= 0 && buf.remaining() > req.getLrecl){
        n = input.read(buf.array(), buf.position(), buf.remaining())
        if (n > 0){
          val pos = buf.position()
          buf.position(pos + n)
          bytesRead += n
        }
      }

      buf.flip()
      hasher.putBytes(buf)
      buf.position(0)
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
    logger.info(s"Closing InputStream - $bytesRead bytes read")
    input.close()

    val response = GRecvResponse.newBuilder
      .setStatus(status)
      .setHash(hasher.hash().toString)
      .setErrCount(errCount)
      .setRowCount(rowCount)
      .build
    val json = JsonFormat.printer().omittingInsignificantWhitespace().print(response)
    val msg = s"request completed $json"
    logger.info(msg)
    orc.close()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
