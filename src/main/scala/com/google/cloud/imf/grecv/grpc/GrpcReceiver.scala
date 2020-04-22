package com.google.cloud.imf.grecv.grpc

import java.util.concurrent.{Executors, TimeUnit}

import com.google.cloud.bqsh.GCS
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.grecv.{GRecvConfig, GRecvProtocol, Receiver}
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.{Logging, TLSUtil}
import com.google.protobuf.util.JsonFormat
import io.grpc.CompressorRegistry
import io.grpc.netty.NettyChannelBuilder

import scala.util.{Failure, Success, Try}

object GrpcReceiver extends Receiver with Logging {
  override def run(cfg: GRecvConfig): Result = {
    try {
      new Service(cfg, GCS.getDefaultClient()).start()
      Result.Success
    } catch {
      case t: Throwable =>
        logger.error(t.getMessage, t)
        Result.Failure(t.getMessage)
    }
  }

  override def recv(req: GRecvProto.GRecvRequest,
                    host: String,
                    port: Int,
                    nConnections: Int,
                    parallelism: Int,
                    tls: Boolean,
                    in: ZRecordReaderT): Result = {
    val cr = CompressorRegistry.newEmptyInstance
    cr.register(new GzipCodec())
    val cb = NettyChannelBuilder.forAddress(host, port)
      .keepAliveTime(10, TimeUnit.MINUTES)
      .compressorRegistry(cr)

    if (parallelism <= 1)
      cb.directExecutor()
    else
      cb.executor(Executors.newWorkStealingPool(parallelism))

    if (!tls) {
      logger.warn("Configuring Client Plaintext")
      cb.usePlaintext()
    } else {
      logger.info("Configuring Client TLS")
      TLSUtil.addConscrypt(cb)
    }

    val ch = new MultiChannel(cb, nConnections)
    val client = new Client(ch)

    val sendResult = Try{client.send(req, in, nConnections)}
    sendResult match {
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Result.Failure(e.getMessage)
      case Success(value) =>
        val responses = value.map(_._1).map(m =>
          JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .print(m)
        ).mkString("\n")
        logger.info("received responses:\n" + responses)
        val checksumResults = value.map{r =>
          val (resp, hash) = r
          logger.debug(s"got response ${resp.getStatus} server: ${resp.getHash} " +
            s"client: $hash")
          val hashMatched = resp.getHash == hash
          if (!hashMatched){
            logger.warn(s"hash mismatch ${resp.getHash} != $hash")
          } else {
            logger.debug(s"checksum valid")
          }
          hashMatched
        }
        if (value.forall(_._1.getStatus == GRecvProtocol.OK))
          Result.Success
        else if (!checksumResults.forall(_ == true))
          Result.Failure(s"checksum failed for ${checksumResults.count(_ == false)}")
        else {
          logger.debug("received non-zero exit code")
          Result.Failure(s"non-zero exit code", value.count(_._1.getStatus != 0))
        }
    }
  }
}
