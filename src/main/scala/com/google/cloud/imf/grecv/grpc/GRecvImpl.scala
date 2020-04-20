package com.google.cloud.imf.grecv.grpc

import java.util.concurrent.atomic.AtomicInteger

import com.google.cloud.imf.gzos.pb.GRecvGrpc.GRecvImplBase
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvResponse, WriteRequest, HealthCheckRequest,
  HealthCheckResponse}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage
import io.grpc.stub.StreamObserver


class GRecvImpl(gcs: Storage) extends GRecvImplBase with Logging {
  private val id: AtomicInteger = new AtomicInteger()

  override def write(responseObserver: StreamObserver[GRecvResponse]): StreamObserver[WriteRequest] = {
    val partId = s"${id.getAndIncrement()}"
    logger.debug("creating GRecvRequestStreamObserver")
    new RequestStream(gcs, partId, responseObserver)
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
