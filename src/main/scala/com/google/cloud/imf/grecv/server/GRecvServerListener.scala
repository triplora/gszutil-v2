package com.google.cloud.imf.grecv.server

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.zip.GZIPInputStream

import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.gzos.{CloudDataSet, Util}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.{Blob, BlobId, Storage}
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path

object GRecvServerListener extends Logging {
  def write(req: GRecvRequest,
            gcs: Storage,
            id: String,
            responseObserver: StreamObserver[GRecvResponse],
            compress: Boolean): Unit = {
    val jobInfo: java.util.Map[String,Any] = Util.toMap(req.getJobinfo)
    val msg1 = s"Received request for ${req.getSrcUri} compress=$compress" +
      s"${JsonFormat.printer.omittingInsignificantWhitespace().print(req.getJobinfo)}"
    logger.info(msg1)

    if (req.getSchema.getFieldCount == 0) {
      responseObserver.onError(Status.FAILED_PRECONDITION
        .withDescription("request has empty schema")
        .asRuntimeException())
      return
    }

    val blob: Blob =
      if (req.getNoData) null
      else {
        val gcsUri = new URI(req.getSrcUri)
        val bucket = gcsUri.getAuthority
        val name = gcsUri.getPath.stripPrefix("/")
        val blobId = BlobId.of(bucket, name)
        val b = gcs.get(blobId)
        if (b == null) {
          val msg = s"${req.getSrcUri} not found"
          logger.error(msg)
          val t = Status.NOT_FOUND.withDescription(msg).asRuntimeException()
          responseObserver.onError(t)
          return
        }
        b
      }

    // Determine record length of input data set
    // LRECL sent with request takes precedence
    // otherwise check object metadata for lrecl
    val lrecl: Int =
      if (req.getLrecl >= 1) req.getLrecl
      else if (req.getNoData) 0
      else {
        val lrecl = blob.getMetadata.get(CloudDataSet.LreclMeta)
        if (lrecl == null) {
          val msg = s"lrecl not set for ${req.getSrcUri}"
          val t = Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException()
          responseObserver.onError(t)
          return
        } else {
          try {
            Integer.valueOf(lrecl)
          } catch {
            case _: NumberFormatException =>
              val msg = s"invalid lrecl '$lrecl' for ${req.getSrcUri}"
              val t = Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException()
              responseObserver.onError(t)
              return
          }
        }
      }

    val input: InputStream = {
      if (req.getNoData){
        // write an empty ORC file which can be registered as an external table
        new ByteArrayInputStream(Array.emptyByteArray)
      } else {
        if (blob != null) open(gcs, blob, compress)
        else {
          val msg = s"${req.getSrcUri} not found"
          logger.error(msg)
          val t = Status.NOT_FOUND.withDescription(msg).asRuntimeException()
          responseObserver.onError(t)
          return
        }
      }
    }

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
    logger.info(s"request completed for ${req.getSrcUri} sending response: $json")
    orc.close()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  def open(gcs: Storage,
           blob: Blob,
           isGzip: Boolean = true,
           bufSz: Int = 2 * 1024 * 1024): InputStream = {
    val is0: InputStream = Channels.newInputStream(gcs.reader(blob.getBlobId))
    val is1: InputStream =
      if (isGzip && !"gzip".equalsIgnoreCase(blob.getContentEncoding))
        new GZIPInputStream(is0, 32 * 1024)
      else
        is0
    new BufferedInputStream(is1, bufSz)
  }
}
