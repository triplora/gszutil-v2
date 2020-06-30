package com.google.cloud.imf.grecv.grpc

import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse
import com.google.cloud.imf.util.Logging
import io.grpc.stub.StreamObserver

import scala.concurrent.Promise
import scala.util.Success

class ResponseStream(promise: Promise[GRecvResponse])
  extends StreamObserver[GRecvResponse] with Logging {
  private var v: GRecvResponse = _
  override def onNext(value: GRecvResponse): Unit = {
    logger.info(s"received GRecvResponse with status ${value.getStatus}")
    v = value
  }
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {
    logger.info("request complete")
    promise.complete(Success(v))
  }
}
