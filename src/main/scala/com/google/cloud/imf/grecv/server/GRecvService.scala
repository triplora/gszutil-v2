package com.google.cloud.imf.grecv.server

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import com.google.cloud.imf.gzos.pb.GRecvGrpc.GRecvImplBase
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, HealthCheckRequest, HealthCheckResponse}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import io.grpc.stub.StreamObserver

import scala.util.Random


class GRecvService(gcs: Storage) extends GRecvImplBase with Logging {
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMddhhMMss")
  private val rng = new Random()

  override def write(request: GRecvRequest, responseObserver: StreamObserver[GRecvResponse]): Unit = {
    val partId = fmt.format(java.time.LocalDateTime.now(ZoneId.of("UTC"))) + "-" +
      rng.alphanumeric.take(6).mkString("")
    logger.debug("creating GRecvRequestStreamObserver")
    try {
      GRecvServerListener.write(request, gcs, partId, responseObserver)
    } catch {
      case t: Throwable =>
        logger.error(t.getMessage, t)
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
