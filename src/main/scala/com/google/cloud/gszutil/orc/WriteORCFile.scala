package com.google.cloud.gszutil.orc

import java.net.URI
import java.nio.channels.ReadableByteChannel

import akka.actor.{ActorSystem, Props}
import akka.io.ByteBufferPool
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.Storage
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await

object WriteORCFile extends Logging {
  def run(gcsUri: String,
          in: ReadableByteChannel,
          copyBook: CopyBook,
          gcs: Storage,
          maxWriters: Int,
          batchSize: Int,
          partSizeMb: Long,
          timeoutMinutes: Int,
          compress: Boolean): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gsz", conf)
    val bufSize = copyBook.LRECL * batchSize
    val pool = ByteBufferPool.allocate(bufSize, maxWriters)
    val args = DatasetReaderArgs(
      in = in,
      batchSize = batchSize,
      uri = new URI(gcsUri),
      maxBytes = partSizeMb*1024*1024,
      nWorkers = maxWriters,
      copyBook = copyBook,
      gcs = gcs,
      compress = compress,
      pool = pool)
    sys.actorOf(Props(classOf[DatasetReader], args), "ZReader")
    Await.result(sys.whenTerminated, atMost = FiniteDuration(timeoutMinutes, MINUTES))
  }
}
