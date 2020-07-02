package com.google.cloud.imf.grecv.client

import java.util.concurrent.{Executor, TimeUnit}

import com.google.api.services.storage.StorageScopes
import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.grecv.{GzipCodec, Uploader}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.gzos.pb.GRecvGrpc.{GRecvBlockingStub, GRecvFutureStub}
import com.google.cloud.imf.gzos.pb.{GRecvGrpc, GRecvProto}
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.{Logging, SecurityUtils, Services}
import com.google.cloud.storage.Storage
import com.google.common.util.concurrent.MoreExecutors
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
    val executor = MoreExecutors.directExecutor()

    val cb = OkHttpChannelBuilder.forAddress(host, port)
      .compressorRegistry(GzipCodec.compressorRegistry)
      .executor(executor)
      .usePlaintext()

    val ch = cb.build()
    val stub = GRecvGrpc.newFutureStub(ch)
      .withExecutor(executor)
      .withCompression("gzip")
      .withDeadlineAfter(60, TimeUnit.SECONDS)
      .withWaitForReady()

    val gcs = Services.storage(zos.getCredentialProvider().getCredentials.createScoped(StorageScopes.DEVSTORAGE_READ_WRITE))

    val sendResult = Try{send(req, in, nConnections, stub, gcs)}
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
           stub: GRecvFutureStub,
           gcs: Storage): Seq[GRecvResponse] = {
    logger.debug(s"sending ${in.getDsn}")

    val streams: Array[GRecvClientListener] = new Array(connections)

    val blksz = in.lRecl * 1024
    val lrecl = in.lRecl
    var count = 0L
    var bytesRead = 0L
    var streamId = 0
    var n = 0

    def stream(i: Int) = {
      var s = streams(i)
      if (s == null) {
        s = new GRecvClientListener(gcs, stub, request, request.getBasepath, blksz)
        streams.update(i, s)
      }
      s
    }

    while (n > -1){
      val buf = stream(streamId).buf
      buf.clear()
      while (n > -1 && buf.remaining >= lrecl){
        n = in.read(buf)
        if (n > 0) {
          bytesRead += n
        }
      }
      if (buf.position() > 0) {
        count += 1
        stream(streamId).flush()
        streamId += 1
        if (streamId >= connections) streamId = 0
      }
    }
    logger.info(s"End of input - $bytesRead bytes")

    val streams1 = streams.filterNot(_ == null)

    logger.debug("Waiting for responses")
    streams1.flatMap(_.collect(90, TimeUnit.SECONDS))
  }
}
