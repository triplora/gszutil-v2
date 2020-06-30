package com.google.cloud.imf.grecv.client

import java.util.concurrent.{Executor, TimeUnit}

import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, WriteRequest}
import com.google.cloud.imf.util.Logging
import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver


/** Sends bytes to server, maintaining a hash of all bytes sent */
class GRecvClientListener(cb: ManagedChannelBuilder[_], request: GRecvRequest, ex: Executor)
  extends StreamObserver[GRecvResponse] with Logging {
  private val ch = cb.build()
  private val stub = GRecvGrpc.newStub(ch)
    .withExecutor(ex)
    .withCompression("gzip")
    .withDeadlineAfter(30, TimeUnit.SECONDS)
    .withWaitForReady()
  val observer = stub.write(this)
  observer.onNext(WriteRequest.newBuilder.setRequest(request).build)
  private val w = WriteRequest.newBuilder
  private val hasher = Hashing.murmur3_128().newHasher()

  def send(buf: Array[Byte], off: Int, len: Int): Unit = {
    logger.debug(s"onNext - ${buf.length} bytes")
    observer.onNext(w.setData(ByteString.copyFrom(buf,0,len)).build)
    hasher.putBytes(buf, 0, len)
  }

  private var v: GRecvResponse = _
  private var isComplete: Boolean = false
  def complete: Boolean = isComplete

  override def onNext(value: GRecvResponse): Unit = {
    v = value
    val s = JsonFormat.printer()
      .includingDefaultValueFields()
      .omittingInsignificantWhitespace()
      .print(value)
    logger.info(s"Received GRecvResponse with status ${value.getStatus}\n$s")
    val hash = hasher.hash().toString
    val hashMatched = value.getHash == hash
    if (!hashMatched){
      val msg = s"Hash mismatch ${value.getHash} != $hash"
      logger.error(msg)
      throw new RuntimeException(msg)
    } else if (value.getStatus != GRecvProtocol.OK) {
      val msg = s"Received status code ${value.getStatus}"
      logger.error(msg)
      throw new RuntimeException(msg)
    }
  }
  override def onError(t: Throwable): Unit = throw t
  override def onCompleted(): Unit = {
    logger.debug(s"onCompleted called")
    isComplete = true
    ch.shutdownNow()
  }

  def result: GRecvResponse = v
}

