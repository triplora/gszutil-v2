package com.google.cloud.gszutil.parallel


import java.util.concurrent.TimeUnit

import akka.actor.{Inbox, Terminated}
import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.storage.BlobInfo

import scala.concurrent.duration.FiniteDuration

object ActorSystem extends Logging {
  sealed trait Message
  case object Start extends Message
  case object Available extends Message
  case class Batch(buf: Array[Byte], limit: Int, partId: Int, lRecl: Int, blkSize: Int) extends Message
  case class Empty(buf: Array[Byte]) extends Message
  case class Free(buf: Array[Byte]) extends Message
  case object Finished extends Message

  def start(prefix: String,
            nWorkers: Int = 20,
            in: ZRecordReaderT,
            copyBook: CopyBook,
            batchSize: Int = 1024,
            partLen: Long = 32 * 1024 * 1024,
            timeoutMinutes: Int = 30): Unit = {
    logger.info(s"initializing actor system")
    val sys = akka.actor.ActorSystem("gszutil")
    logger.info(s"creating master actor")
    val inbox = Inbox.create(sys)
    val master = sys.actorOf(Manager.props(nWorkers, prefix, batchSize, partLen, in, copyBook))
    inbox.watch(master)
    logger.info(s"timeout set to $timeoutMinutes minutes")
    while (true) {
      inbox.receive(FiniteDuration.apply(length = timeoutMinutes, unit = TimeUnit.MINUTES)) match {
        case Terminated =>
          logger.warn(s"terminating actor system")
          sys.terminate()
          Thread.sleep(1000)
          System.exit(0)
        case x =>
          logger.warn(s"received unexpected message $x")
      }
    }
  }

  def getUri(blobInfo: BlobInfo): String =
    s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}"
}
