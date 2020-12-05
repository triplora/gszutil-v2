package com.google.cloud.imf.grecv.server

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.zip.GZIPInputStream

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.{CloudDataSet, Util}
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.{BlobId, Storage}
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

object GRecvServerListener extends Logging {
  def write(req: GRecvRequest,
            gcs: Storage,
            id: String,
            responseObserver: StreamObserver[GRecvResponse],
            compress: Boolean): Unit = {
    val jobInfo: java.util.Map[String,Any] = Util.toMap(req.getJobinfo)
    val msg1 = "Received request for " + req.getSrcUri + " " +
      JsonFormat.printer.omittingInsignificantWhitespace().print(req.getJobinfo)
    logger.info(msg1)

    if (req.getSchema.getFieldCount == 0) throw new RuntimeException("empty schema")

    // Determine record length of input data set
    // LRECL sent with request takes precedence
    // otherwise check object metadata for lrecl
    val lrecl: Int =
      if (req.getLrecl >= 1) req.getLrecl
      else if (req.getNoData) 0
      else {
        val gcsUri = new URI(req.getSrcUri)
        val bucket = gcsUri.getAuthority
        val name = gcsUri.getPath.stripPrefix("/")
        val blob = gcs.get(BlobId.of(bucket, name))
        if (blob == null)
          throw new RuntimeException(s"GCS object not found. uri=$gcsUri")
        val lrecl = blob.getMetadata.get(CloudDataSet.LreclMeta)
        if (lrecl == null)
          throw new RuntimeException("lrecl not set")
        else if (lrecl.length < 1 || lrecl.length > 5)
          throw new RuntimeException(s"invalid lrecl length ${lrecl.length}")
        else if (lrecl.forall(_.isDigit))
          throw new RuntimeException(s"invalid lrecl $lrecl")
        lrecl.toInt
      }

    val input: InputStream = {
      if (req.getNoData){
        // write an empty ORC file which can be registered as an external table
        new ByteArrayInputStream(Array.emptyByteArray)
      } else {
        val gcsUri = new URI(req.getSrcUri)
        val bucket = gcsUri.getAuthority
        val name = gcsUri.getPath.stripPrefix("/")
        val is: InputStream = Channels.newInputStream(gcs.reader(bucket,name))
        if (compress) new BufferedInputStream(new GZIPInputStream(is, 32 * 1024), 2 * 1024 * 1024)
        else new BufferedInputStream(is, 2 * 1024 * 1024)
      }
    }
    logger.debug(s"Opened ${req.getSrcUri}")

    val hasher = Hashing.murmur3_128().newHasher()
    val buf: ByteBuffer = ByteBuffer.allocate(lrecl*1024)

    val orc: WriterCore = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      gcs = gcs,
      name = s"$id",
      lrecl = lrecl)
    var errCount: Long = 0
    var rowCount: Long = 0
    var bytesRead: Long = 0
    var status: Int = GRecvProtocol.OK

    logger.debug(s"starting to write")
    var n = 0
    while (n > -1) {
      buf.clear()
      while (n > -1 && buf.hasRemaining){
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
      errCount += result.errCount
      rowCount += result.rowCount

      val errPct = errCount.doubleValue() / math.max(1,rowCount)
      if (errPct > req.getMaxErrPct) {
        logger.debug(s"errPct $errPct")
        status = GRecvProtocol.ERR
      }
    }
    logger.info(s"Finished reading $bytesRead bytes from ${req.getSrcUri}; wrote $rowCount rows")
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
