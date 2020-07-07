package com.google.cloud.imf.grecv.client

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

import com.google.auth.oauth2.AccessToken
import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.{Logging, StorageObjectOutputStream}
import com.google.common.hash.{Hasher, Hashing}
import io.grpc.okhttp.OkHttpChannelBuilder

import scala.util.Random

class TmpObj(bucket: String,
             tmpPath: String,
             token: AccessToken,
             cb: OkHttpChannelBuilder,
             request: GRecvRequest,
             limit: Long,
             compress: Boolean) extends Logging {
  private val name = tmpPath + Random.alphanumeric.take(32).mkString("")
  private val srcUri = s"gs://$bucket/$name"
  logger.debug(s"Opening $srcUri for writing")
  private val hasher: Hasher = Hashing.murmur3_128().newHasher()
  private val os: StorageObjectOutputStream = new StorageObjectOutputStream(token, bucket, name)
  private val writer: OutputStream =
    if (compress) new GZIPOutputStream(os, 32*1024, true)
    else os
  //private val writer = os
  private var closed: Boolean = false

  def isClosed: Boolean = closed
  def getCount: Long = os.getCount

  def write(buf: ByteBuffer): Unit = {
    val n = buf.limit()
    val a = buf.array()
    hasher.putBytes(a, 0, n)
    writer.write(a, 0, n)
    buf.position(n)
    if (os.getCount > limit){
      logger.debug(s"part limit $limit reached ${os.getCount}")
      close()
    }
  }

  def close(): Unit = {
    closed = true
    logger.info(s"closing $srcUri")
    writer.close()
    val n = os.getCount
    if (n > 0){
      logger.info(s"Finished writing $n bytes to $srcUri")
      val hash = hasher.hash().toString
      logger.debug(s"Requesting write to ORC for $srcUri")
      val ch = cb.build()
      val stub = GRecvGrpc.newBlockingStub(ch)
        .withCompression("gzip")
        .withDeadlineAfter(240, TimeUnit.SECONDS)
        .withWaitForReady()
      val res = stub.write(request.toBuilder.setSrcUri(srcUri).build())
      ch.shutdownNow()
      require(res.getHash == hash, "hash mismatch")
      require(res.getStatus == GRecvProtocol.OK, "non-success status code")
    }
  }
}