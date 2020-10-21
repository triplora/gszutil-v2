package com.google.cloud.imf.grecv.client

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

import com.google.cloud.imf.grecv.GRecvProtocol
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.{BlobInfo, Storage}
import com.google.common.hash.{Hasher, Hashing}
import com.google.common.io.CountingOutputStream
import io.grpc.okhttp.OkHttpChannelBuilder

import scala.util.Random

case class TmpObj(bucket: String,
             tmpPath: String,
             gcs: Storage,
             cb: OkHttpChannelBuilder,
             request: GRecvRequest,
             limit: Long,
             compress: Boolean) extends Logging {
  private val name = s"${tmpPath}_${System.currentTimeMillis()}_${Random.alphanumeric.take(8)}"
  private val srcUri = s"gs://$bucket/$name"
  logger.debug(s"Opening $srcUri for writing")
  private val hasher: Hasher = Hashing.murmur3_128().newHasher()

  // Use a CountingOutputStream to count compressed bytes
  private val os: CountingOutputStream = new CountingOutputStream(
    Channels.newOutputStream(gcs.writer(
      BlobInfo.newBuilder(bucket,name).build(),
      BlobWriteOption.doesNotExist())))

  private val writer: OutputStream =
    if (compress) new GZIPOutputStream(os, 32*1024, true)
    else os
  private var closed: Boolean = false

  def isClosed: Boolean = closed

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
        .withDeadlineAfter(3000, TimeUnit.SECONDS)
      // send the request to the gRPC server, causing it to transcode to ORC
      val res = stub.write(request.toBuilder.setSrcUri(srcUri).build())
      ch.shutdownNow()
      logger.info(s"Request complete. uri=$srcUri rowCount=${res.getRowCount} " +
        s"errors=${res.getErrCount}")
      require(res.getHash == hash, "hash mismatch")
      require(res.getStatus == GRecvProtocol.OK, "non-success status code")
    } else {
      logger.info(s"Requesting write empty ORC file at $srcUri")
      val ch = cb.build()
      val stub = GRecvGrpc.newBlockingStub(ch)
        .withDeadlineAfter(3000, TimeUnit.SECONDS)
      // send the request to the gRPC server, causing it to write an empty file
      val res = stub.write(request.toBuilder.setNoData(true).build())
      ch.shutdownNow()
      logger.info(s"Request complete. uri=$srcUri rowCount=${res.getRowCount} " +
        s"errors=${res.getErrCount}")
      require(res.getStatus == GRecvProtocol.OK, "non-success status code")
    }
  }
}
