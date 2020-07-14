package com.google.cloud.imf.grecv.client

import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.{GoogleCredentials, OAuth2Credentials}
import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.grecv.{GzipCodec, Uploader}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.protobuf.ByteString
import io.grpc.okhttp.OkHttpChannelBuilder

import scala.util.{Failure, Success, Try}

object GRecvClient extends Uploader with Logging {
  def run(cfg: GsUtilConfig,
          zos: MVS,
          in: ZRecordReaderT,
          schemaProvider: SchemaProvider,
          receiver: Uploader): Result = {
    logger.info("Starting Dataset Upload")

    try {
      logger.info("Starting Send...")
      val keypair = zos.getKeyPair()
      val req = GRecvRequest.newBuilder
        .setSchema(schemaProvider.toRecordBuilder.build)
        .setBasepath(cfg.gcsUri)
        .setLrecl(in.lRecl)
        .setBlksz(in.blkSize)
        .setMaxErrPct(cfg.maxErrorPct)
        .setJobinfo(zos.getInfo)
        .setPrincipal(zos.getPrincipal())
        .setPublicKey(ByteString.copyFrom(keypair.getPublic.getEncoded))
        .setTimestamp(System.currentTimeMillis())

      req.setSignature(ByteString.copyFrom(SecurityUtils.sign(keypair.getPrivate,
        req.clearSignature.build.toByteArray)))

      receiver.upload(req.build, cfg.remoteHost, cfg.remotePort, cfg.nConnections, zos, in)
    } catch {
      case e: Throwable =>
        logger.error("Dataset Upload Failed", e)
        Result.Failure(e.getMessage)
    }
  }

  override def upload(req: GRecvProto.GRecvRequest,
                      host: String,
                      port: Int,
                      nConnections: Int,
                      zos: MVS,
                      in: ZRecordReaderT): Result = {
    val cb = OkHttpChannelBuilder.forAddress(host, port)
      .compressorRegistry(GzipCodec.compressorRegistry)
      .usePlaintext()
      .keepAliveTime(240, TimeUnit.SECONDS)
      .keepAliveWithoutCalls(true)

    val creds: GoogleCredentials = zos.getCredentialProvider().getCredentials
      .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)
    creds.refreshIfExpired()

    val sendResult = Try{send(req, in, nConnections, cb, creds)}
    sendResult match {
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Result.Failure(e.getMessage)
      case Success(_) =>
        Result.Success
    }
  }

  def send(request: GRecvRequest,
           in: ZRecordReaderT,
           connections: Int = 1,
           cb: OkHttpChannelBuilder,
           creds: OAuth2Credentials): Unit = {
    logger.debug(s"sending ${in.getDsn}")

    val blksz = in.lRecl * 1024
    val partLimit = 64*1024*1024
    var bytesRead = 0L
    var streamId = 0
    var n = 0
    val baseUri = new URI(request.getBasepath)

    val streams: Array[GRecvClientListener] = (0 until connections).toArray
      .map(_ => new GRecvClientListener(creds, cb, request, baseUri, blksz, partLimit))

    while (n > -1){
      val buf = streams(streamId).buf
      buf.clear()
      while (n > -1 && buf.hasRemaining){
        n = in.read(buf)
        if (n > 0) {
          bytesRead += n
        }
      }
      if (buf.position() > 0) {
        streams(streamId).flush()
        if (bytesRead >= 1024*1024) {
          streamId += 1
          if (streamId >= connections) streamId = 0
        }
      }
    }
    val streams1 = streams.filterNot(_ == null)

    logger.info(s"Finished reading $bytesRead bytes from ${in.getDsn}")
    streams1.foreach(_.close())
  }
}
