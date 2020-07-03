package com.google.cloud.imf.grecv.client

import java.io.OutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.{Executor, TimeUnit}
import java.util.zip.GZIPOutputStream

import com.google.cloud.WriteChannel
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvGrpc.{GRecvBlockingStub, GRecvFutureStub, GRecvStub}
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage}
import com.google.common.hash.{Hasher, Hashing}
import com.google.common.util.concurrent.ListenableFuture
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import io.grpc.{CallOptions, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver

import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}


/** Sends bytes to server, maintaining a hash of all bytes sent */
class GRecvClientListener(gcs: Storage,
                          stub: GRecvFutureStub,
                          request: GRecvRequest,
                          basePath: String, bufSz: Int) extends Logging {
  val buf: ByteBuffer = ByteBuffer.allocate(bufSz)
  val partLimit: Long = 1L*1024*1024*1024
  // a hash and a future response
  val futures: ListBuffer[(String,ListenableFuture[GRecvResponse])] = ListBuffer.empty

  def newObj(): Blob = {
    val bucket = new URI(basePath).getAuthority
    val path = new URI(basePath).getPath.stripPrefix("/")+Random.alphanumeric.take(32).mkString("")
    logger.debug(s"opening gs://$bucket/$path")
    gcs.create(BlobInfo.newBuilder(BlobId.of(bucket,path)).build())
  }

  var hasher: Hasher = Hashing.murmur3_128().newHasher()
  var obj: Blob = _
  var writer: OutputStream = _
  var bytesWritten: Long = 0

  def getWriter(): OutputStream = {
    if (obj == null || writer == null) {
      obj = newObj()
      writer = new GZIPOutputStream(Channels.newOutputStream(obj.writer()), 32*1024, true)
    }
    writer
  }

  def flush(): Unit = {
    buf.flip()
    val nBytes = buf.limit()
    hasher.putBytes(buf.array(), 0, nBytes)
    getWriter().write(buf.array(), 0, nBytes)
    bytesWritten += nBytes
    if (bytesWritten > partLimit)
      close()
  }

  def close(): Unit = {
    val src = "gs://" + obj.getBucket + "/" + obj.getName
    logger.info(s"closing $src")
    if (writer != null) {
      writer.close()
      writer = null
      obj = null
    }
    if (bytesWritten > 0){
      val hash = hasher.hash().toString
      logger.debug(s"sending request for $src")
      val futureResponse = stub.write(request.toBuilder.setSrcUri(src).build())
      futures.append((hash, futureResponse))
    } else {
      logger.debug(s"deleting gs://${obj.getBlobId.getBucket}/${obj.getBlobId.getName}")
      try{gcs.delete(obj.getBlobId)}
      catch {case t: Throwable =>}
    }
    bytesWritten = 0
    hasher = Hashing.murmur3_128().newHasher()
  }

  def collect(timeout: Int, timeUnit: TimeUnit): List[GRecvResponse] = {
    close()
    futures.map(x => (x._1, x._2.get(timeout, timeUnit)))
      .map(x => checkResponse(x._1, x._2))
      .toList
  }

  def checkResponse(hash: String, value: GRecvResponse): GRecvResponse = {
    val s = JsonFormat.printer()
      .includingDefaultValueFields()
      .omittingInsignificantWhitespace()
      .print(value)
    logger.info(s"Received GRecvResponse with status ${value.getStatus}\n$s")
    val hashMatched = value.getHash == hash
    if (!hashMatched){
      val msg = s"Hash mismatch ${value.getHash} != $hash"
      logger.error(msg)
      throw new RuntimeException(msg)
    } else if (value.getStatus != GRecvProtocol.OK) {
      val msg = s"Received status code ${value.getStatus}"
      logger.error(msg)
      throw new RuntimeException(msg)
    } else value
  }
}

