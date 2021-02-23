package com.google.cloud.imf.grecv.client

import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.{GoogleCredentials, OAuth2Credentials}
import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.{CloudRecordReader, ZRecordReaderT}
import com.google.cloud.imf.grecv.{GRecvProtocol, Uploader}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.{GzipCodec, Logging, Services}
import com.google.protobuf.util.JsonFormat
import io.grpc.okhttp.OkHttpChannelBuilder

import scala.util.{Failure, Success, Try}

object GRecvClient extends Uploader with Logging {
  def run(cfg: GsUtilConfig,
          zos: MVS,
          in: ZRecordReaderT,
          schemaProvider: SchemaProvider,
          receiver: Uploader): Result = {
    logger.info(s"GRecvClient Starting Dataset Upload to ${cfg.gcsUri}")

    try {
      val req = GRecvRequest.newBuilder
        .setSchema(schemaProvider.toRecordBuilder.build)
        .setBasepath(cfg.gcsUri) // where to write output
        .setLrecl(in.lRecl)
        .setBlksz(in.blkSize)
        .setMaxErrPct(cfg.maxErrorPct)
        .setJobinfo(zos.getInfo)
        .setPrincipal(zos.getPrincipal())
        .setTimestamp(System.currentTimeMillis())

      receiver.upload(req.build, cfg.remoteHost, cfg.remotePort, cfg.nConnections, zos, in)
    } catch {
      case e: Throwable =>
        logger.error("Dataset Upload Failed", e)
        Result.Failure(e.getMessage)
    }
  }

  override def upload(req: GRecvRequest,
                      host: String,
                      port: Int,
                      nConnections: Int,
                      zos: MVS,
                      in: ZRecordReaderT): Result = {
    val cb = OkHttpChannelBuilder.forAddress(host, port)
      .compressorRegistry(GzipCodec.compressorRegistry)
      .usePlaintext() // TLS is provided by AT-TLS
      .keepAliveTime(240, TimeUnit.SECONDS)
      .keepAliveWithoutCalls(true)

    val creds: GoogleCredentials = zos.getCredentialProvider().getCredentials
      .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)
    creds.refreshIfExpired()

    Try{
      in match {
        case x: CloudRecordReader =>
          gcsSend(req, x, cb, creds)
        case _ =>
          send(req, in, nConnections, cb, creds)
      }
    } match {
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Result.Failure(e.getMessage)
      case Success(result) => result
    }
  }

  private val PartLimit: Int = 64*1024*1024

  def send(request: GRecvRequest,
           in: ZRecordReaderT,
           connections: Int = 1,
           cb: OkHttpChannelBuilder,
           creds: OAuth2Credentials): Result = {
    logger.debug(s"Sending ${in.getDsn}")

    val blksz = in.lRecl * 1024
    var bytesRead = 0L
    var streamId = 0
    val baseUri = new URI(request.getBasepath.stripSuffix("/"))

    val gcs = Services.storage(creds)
    val streams: Array[GRecvClientListener] = (0 until connections).toArray
      .map(_ => new GRecvClientListener(gcs, cb, request, baseUri, blksz, PartLimit))

    var n = 0
    while (n > -1){
      val buf = streams(streamId).buf
      buf.clear()
      while (n > -1 && buf.hasRemaining){
        n = in.read(buf)
        bytesRead += n
      }
      if (buf.position() > 0) {
        streams(streamId).flush()
        if (bytesRead >= 1024*1024) {
          streamId += 1
          if (streamId >= connections) streamId = 0
        }
      }
    }
    if (n < 0) bytesRead -= n

    streams.foreach(_.close())
    if (bytesRead < 1){
      logger.info(s"Read $bytesRead bytes from ${in.getDsn} - requesting empty file be written")
      val ch = cb.build()
      try {
        val stub = GRecvGrpc.newBlockingStub(ch).withDeadlineAfter(3000, TimeUnit.SECONDS)
        val res = stub.write(request.toBuilder.setNoData(true).build())
        if (res.getStatus != GRecvProtocol.OK)
          Result.Failure("non-success status code")
        else {
          val resStr = JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .print(res)
          Result(activityCount = res.getRowCount, message = s"Request completed. DSN=${in.getDsn} " +
            s" rowCount=${res.getRowCount} errorCount=${res.getErrCount} $resStr")
        }
      } finally {
        ch.shutdownNow()
      }
    } else {
      // Each TmpObj instance sends its own request in close() method
      val msg = s"Read $bytesRead bytes from DSN:${in.getDsn}"
      logger.info(msg)
      Result(activityCount = bytesRead / in.lRecl, message = msg)
    }
  }

  /** Request transcoding of a dataset already uploaded
    * to Cloud Storage
    *
    * @param request GRecvRequest template
    * @param in CloudRecordReader with reference to DSN
    * @param cb OkHttpChannelBuilder used to create a gRPC client
    * @param creds OAuth2Credentials used to generate OAuth2 AccessToken
    * @return
    */
  def gcsSend(request: GRecvRequest,
              in: CloudRecordReader,
              cb: OkHttpChannelBuilder,
              creds: OAuth2Credentials): Result = {

    def gcsSendImpl: String => Result = (uri: String) => {
      var rowCount: Long = 0
      var errCount: Long = 0
      val ch = cb.build()
      try {
        val stub = GRecvGrpc.newBlockingStub(ch)
          .withDeadlineAfter(90, TimeUnit.MINUTES)
        val req = request.toBuilder.setSrcUri(uri).build()
        logger.info(
          s"""Sending GRecvRequest request
             |in:${req.getSrcUri}
             |out:${req.getBasepath}""".stripMargin)
        val res = stub.write(req)
        if (res.getRowCount > 0)
          rowCount += res.getRowCount
        if (res.getErrCount > 0)
          errCount += res.getErrCount
        if (res.getStatus != GRecvProtocol.OK)
          Result.Failure("non-success status code")
        else {
          val resStr = JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .print(res)
          logger.info(s"Request complete. DSN=${in.dsn}, uri=$uri rowCount=${res.getRowCount} " +
            s"errorCount=${res.getErrCount} $resStr")
          Result(activityCount = rowCount, message = s"Completed with " +
            s"$errCount errors")
        }
      } catch {
        case t: Throwable =>
          Result.Failure(s"GRecv failure: ${t.getMessage}")
      } finally {
        ch.shutdownNow()
      }
    }

    if(in.gdg) {
      logger.debug(s"Sending GRecvRequest request for GDG ${in.versions.size} versions")
      val results = in.versions.map {v =>
        val uri = in.gdgUri(v.getName)
        val result = gcsSendImpl(uri)
        logger.info(s"GRecvRequest for $uri completed with exit code ${result.exitCode}")
        result
      }
      results.find(r => r.exitCode != 0) match {
        case Some(failed) => failed
        case None => results.head
      }
    } else {
      gcsSendImpl(in.uri)
    }
  }
}
