package com.google.cloud.gszutil.parallel


import java.util.concurrent.TimeUnit

import akka.actor.{Inbox, Terminated}
import com.google.cloud.storage.{BlobInfo, Storage}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

object ActorSystem {
  sealed trait Message
  case object Start extends Message
  case object Available extends Message
  case class Batch(buf: Array[Byte], limit: Int, partId: Int) extends Message
  case object Finished extends Message

  private val log = LoggerFactory.getLogger(getClass)

  def start(prefix: String,
            storage: Storage,
            nWorkers: Int = 20,
            dd: String = "INFILE",
            batchSize: Int = 1024,
            partLen: Long = 32 * 1024 * 1024,
            timeoutMinutes: Int = 30): Unit = {
    log.info(s"initializing actor system")
    val sys = akka.actor.ActorSystem("gszutil")
    log.info(s"creating master actor")
    val inbox = Inbox.create(sys)
    val master = sys.actorOf(Master.props(nWorkers, dd, prefix, storage, batchSize, partLen))
    inbox.watch(master)
    log.info(s"timeout set to $timeoutMinutes minutes")
    inbox.receive(FiniteDuration.apply(length = timeoutMinutes, unit = TimeUnit.MINUTES)) match {
      case Terminated =>
        log.warn(s"received Terminated")
      case x =>
        log.warn(s"received unexpected message $x")
    }
    log.warn(s"terminating actor system")
    sys.terminate()
  }

  def getUri(blobInfo: BlobInfo): String =
    s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}"
}
