package com.google.cloud.imf.grecv.grpc

import com.google.cloud.imf.gzos.pb.GRecvGrpc.GRecvStub
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse, WriteRequest}
import com.google.common.hash.Hashing
import com.google.protobuf.ByteString

import scala.concurrent.Promise


case class WriteStream(asyncStub: GRecvStub, id: String) {
  val response = Promise[GRecvResponse]()
  private val out = asyncStub.write(new ResponseStream(response))
  private val w = WriteRequest.newBuilder
  private val hasher = Hashing.murmur3_128().newHasher()
  def result: String =
    if (hasher != null) hasher.hash().toString
    else ""

  def init(request: GRecvRequest): Unit = {
    out.onNext(WriteRequest.newBuilder.setRequest(request).build)
  }

  def onNext(buf: Array[Byte], off: Int, len: Int): Unit = {
    out.onNext(w.setData(ByteString.copyFrom(buf,0,len)).build)
    hasher.putBytes(buf, 0, len)
  }
  def onCompleted(): Unit = out.onCompleted()
}

