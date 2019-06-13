package com.google.cloud.gszutil.orc

import java.net.URI

import akka.actor.{ActorSystem, Props}
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZChannel, ZRecordReaderT}
import com.google.cloud.storage.Storage
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await

object WriteORCFile extends Logging {
  def run(gcsUri: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          gcs: Storage,
          maxWriters: Int = 5,
          batchSize: Int = 10000,
          partLen: Long = 128 * 1024 * 1024,
          timeoutMinutes: Int = 60): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gsz", conf)
    val args: DatasetReaderArgs = DatasetReaderArgs(new ZChannel(in), in.lRecl, batchSize, new URI(gcsUri), partLen, maxWriters, copyBook, gcs)
    sys.actorOf(Props(classOf[DatasetReader], args), "ZReader")
    Await.result(sys.whenTerminated, atMost = FiniteDuration(timeoutMinutes, MINUTES))
  }
}