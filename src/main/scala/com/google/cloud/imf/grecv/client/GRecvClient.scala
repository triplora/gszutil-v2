package com.google.cloud.imf.grecv.client

import java.util.concurrent.Executor

import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.grecv.{GzipCodec, Uploader}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
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

      receiver.upload(req.build, cfg.remoteHost, cfg.remotePort, cfg.nConnections, in)
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
                      in: ZRecordReaderT): Result = {
    val executor = MoreExecutors.directExecutor()

    val cb = OkHttpChannelBuilder.forAddress(host, port)
      .compressorRegistry(GzipCodec.compressorRegistry)
      .executor(executor)
      .usePlaintext()

    val sendResult = Try{send(req, in, nConnections, cb, executor)}
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
           cb: ManagedChannelBuilder[_],
           ex: Executor): Seq[GRecvResponse] = {
    logger.debug(s"sending ${in.getDsn}")

    val streams: Array[GRecvClientListener] = new Array(connections)

    def stream(i: Int) = {
      var s = streams(i)
      if (s == null) {
        s = new GRecvClientListener(cb, request, ex)
        streams.update(i, s)
      }
      s
    }

    val blksz = in.lRecl * 1024
    val lrecl = in.lRecl
    val buf = new Array[Byte](blksz)
    var count = 0L
    var bytesRead = 0L
    var streamId = 0
    var n = 0
    var pos = 0

    while (n > -1){
      pos = 0
      while (n > -1 && blksz - pos >= lrecl){
        n = in.read(buf, pos, lrecl)
        if (n > 0) {
          bytesRead += n
          pos += n
        }
      }
      if (pos > 0) {
        count += 1
        stream(streamId).send(buf,0,pos)
        streamId += 1
        if (streamId >= connections) streamId = 0
      }
    }
    logger.info(s"End of stream - $bytesRead bytes")

    val streams1 = streams.filterNot(_ == null)
    streams1.foreach(_.observer.onCompleted())
    logger.debug("Waiting for onComplete callback")

    val timeout = System.currentTimeMillis() + 20000L
    var yieldCount = 0L
    while (!streams1.forall(_.complete)){
      Thread.`yield`()
      yieldCount += 1
      if (System.currentTimeMillis() > timeout)
        throw new RuntimeException("timed out waiting for onComplete callback")
    }
    logger.debug(s"yielded $yieldCount times")
    streams1.map(_.result)
  }
}
