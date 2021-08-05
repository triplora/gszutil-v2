package com.google.cloud.imf.grecv.server

import com.google.api.services.storage.{Storage => LowLevelStorageApi}
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.gszutil.io.WriterCore
import com.google.cloud.gszutil.io.exports._
import com.google.cloud.gszutil.{CopyBook, RecordSchema, SchemaProvider}
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.gzos.{CloudDataSet, Ebcdic, Util}
import com.google.cloud.imf.util.{Logging, ServiceLogger}
import com.google.cloud.storage.{Blob, BlobId, Storage}
import com.google.common.hash.Hashing
import com.google.protobuf.util.JsonFormat
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path
import com.google.cloud.imf.util.RetryHelper._

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.zip.GZIPInputStream
import scala.util.Try

object GRecvServerListener extends Logging {
  def write(req: GRecvRequest,
            gcs: Storage,
            lowLevelStorageApi: LowLevelStorageApi,
            id: String,
            responseObserver: StreamObserver[GRecvResponse],
            compress: Boolean)(implicit log: ServiceLogger): Unit = {
    val jobInfo: java.util.Map[String, Any] = Util.toMap(req.getJobinfo)
    val msg1 = s"Received request for ${req.getSrcUri} compress=$compress" +
      s"${JsonFormat.printer.omittingInsignificantWhitespace().print(req.getJobinfo)}"
    log.info(msg1)

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
          log.error(msg)
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
          log.error(msg)
          val t = Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException()
          responseObserver.onError(t)
          return
        } else {
          try {
            Integer.valueOf(lrecl)
          } catch {
            case _: NumberFormatException =>
              val msg = s"invalid lrecl '$lrecl' for ${req.getSrcUri}"
              log.error(msg)
              val t = Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException()
              responseObserver.onError(t)
              return
          }
        }
      }

    val orc: WriterCore = new WriterCore(schemaProvider = RecordSchema(req.getSchema),
      basePath = new Path(req.getBasepath),
      gcs = gcs,
      name = s"$id",
      lrecl = lrecl)

    val input: InputStream = {
      if (req.getNoData){
        // write an empty ORC file which can be registered as an external table
        new ByteArrayInputStream(Array.emptyByteArray)
      } else {
        if (blob != null) open(gcs, lowLevelStorageApi, blob, compress)
        else {
          val msg = s"${req.getSrcUri} not found"
          log.error(msg)
          val t = Status.NOT_FOUND.withDescription(msg).asRuntimeException()
          responseObserver.onError(t)
          return
        }
      }
    }

    try {
      val hasher = Hashing.murmur3_128().newHasher()
      val buf: ByteBuffer = ByteBuffer.allocate(lrecl*1024)

      var errCount: Long = 0
      var rowCount: Long = 0
      var bytesRead: Long = 0
      var status: Int = GRecvProtocol.OK

      log.info("starting to write")
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
          log.info(s"errPct $errPct")
          status = GRecvProtocol.ERR
        }
      }

      log.info(s"Finished reading $bytesRead bytes from ${req.getSrcUri}; wrote $rowCount rows")
      val response = GRecvResponse.newBuilder
        .setStatus(status)
        .setHash(hasher.hash().toString)
        .setErrCount(errCount)
        .setRowCount(rowCount)
        .build
      val json = JsonFormat.printer().omittingInsignificantWhitespace().print(response)
      log.info(s"request completed for ${req.getSrcUri} sending response: $json")
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    } finally {
      retryable(orc.close(), s"Closing resources for orc writer ${req.getBasepath}.")
      retryable(input.close(), s"Closing resources for inputStream ${req.getSrcUri}.")
    }
  }

  def export(request: GRecvProto.GRecvExportRequest,
             bq: BigQuery,
             storageApi: BigQueryReadClient,
             gcs: Storage,
             cfg: ExportConfig,
             responseObserver: StreamObserver[GRecvResponse])(implicit log: ServiceLogger) : Unit = {

    val sp = parseCopybook(request.getCopybook)
    val gcsFunc = (x: String) => GcsFileExport(gcs, s"${request.getOutputUri.stripSuffix("/")}/${request.getJobinfo.getJobid}/tmp_$x", sp.LRECL)
    val result = if(cfg.runMode.toLowerCase == "single") {
      log.info(s"Export mode - single thread")
      new BqSelectResultExporter(cfg, bq, request.getJobinfo, sp, GcsFileExport(gcs, request.getOutputUri, sp.LRECL)).doExport(request.getSql)
    } else if("storage_api" == cfg.runMode.toLowerCase) {
      log.info(s"Export mode - storage API")

      new BqStorageApiExporter(cfg.copy(exporterThreadCount = Runtime.getRuntime.availableProcessors()), storageApi, bq, gcsFunc, request.getJobinfo, sp)
        .doExport(request.getSql)
    } else {
      log.info(s"Export mode - multiple threads")

      def exporterFactory(fileSuffix: String, cfg: ExportConfig): SimpleFileExporter = {
        val result = new LocalFileExporter
        result.newExport(gcsFunc(fileSuffix))
        new SimpleFileExporterAdapter(result, cfg)
      }

      new BqSelectResultParallelExporter(cfg.copy(exporterThreadCount = Runtime.getRuntime.availableProcessors()), bq, request.getJobinfo, sp, exporterFactory)
        .doExport(request.getSql)
    }

    val resp = GRecvResponse.newBuilder.setStatus(GRecvProtocol.OK).setRowCount(result.activityCount).build
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  private def parseCopybook(copybook: String)(implicit log: ServiceLogger): SchemaProvider =
    Try(CopyBook(copybook, Ebcdic)).toEither match {
      case Right(sp) => {
        log.info(s"Loaded copybook with LRECL=${sp.LRECL}")
        sp
      }
      case Left(e) => throw new RuntimeException("Failed to parse copybook", e)
    }

  def open(gcs: Storage,
           lowLevelStorageApi: LowLevelStorageApi,
           blob: Blob,
           isGzip: Boolean = true,
           bufSz: Int = 2 * 1024 * 1024)(implicit log: ServiceLogger): InputStream = {
    val contentEncoding = Option(blob.getContentEncoding).map(_.toLowerCase)
    log.info(s"Requesting input stream, contentEncoding=${contentEncoding.getOrElse("")} blobId=${blob.getBlobId} blobSIze=${blob.getSize}")
    val is0: InputStream = contentEncoding match {
      case Some("gzip") => lowLevelStorageApi.objects().get(blob.getBlobId.getBucket, blob.getBlobId.getName).executeMediaAsInputStream()
      case _ => Channels.newInputStream(gcs.reader(blob.getBlobId))
    }
    val is1: InputStream =
      if (isGzip && !contentEncoding.exists(_.equals("gzip")))
        new GZIPInputStream(is0, 32 * 1024)
      else
        is0
    new BufferedInputStream(is1, bufSz)
  }
}
