package com.google.cloud.imf.grecv.server

import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.google.api.services.storage.{Storage => LowLevelStorageApi}
import com.google.cloud.imf.gzos.pb.GRecvGrpc.GRecvImplBase
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, HealthCheckRequest, HealthCheckResponse}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import com.google.protobuf.util.JsonFormat
import io.grpc.stub.StreamObserver

import scala.util.Random


class GRecvService(private val gcs: Storage, private val lowLevelStorageApi: LowLevelStorageApi) extends GRecvImplBase with Logging {
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMddhhMMss")
  private val rng = new Random()

  override def write(request: GRecvRequest, responseObserver: StreamObserver[GRecvResponse]): Unit = {
    val partId = fmt.format(java.time.LocalDateTime.now(ZoneId.of("UTC"))) + "-" +
      rng.alphanumeric.take(6).mkString("")
    val requestJson = JsonFormat.printer().omittingInsignificantWhitespace().print(request)
    logger.debug(s"Received GRecvRequest\n```\n$requestJson\n```")
    try {
      GRecvServerListener.write(request, gcs, lowLevelStorageApi, partId, responseObserver, compress = true)
    } catch {
      case t: Throwable =>
        logger.error(t.getMessage, t)
        val t1 = io.grpc.Status.INTERNAL
          .withDescription(t.getMessage)
          .withCause(t)
          .asRuntimeException()
        responseObserver.onError(t1)
    }
  }

  private val OK: HealthCheckResponse =
    HealthCheckResponse.newBuilder
      .setStatus(HealthCheckResponse.ServingStatus.SERVING)
      .build

  override def check(req: HealthCheckRequest,
                     res: StreamObserver[HealthCheckResponse]): Unit = {
    res.onNext(OK)
    res.onCompleted()
  }
}
