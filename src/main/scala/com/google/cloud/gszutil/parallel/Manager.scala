package com.google.cloud.gszutil.parallel

import java.net.URI

import akka.actor.{Actor, ActorRef, Props, Terminated}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.gszutil.parallel.ActorSystem.{Available, Finished}
import com.google.cloud.storage.{BlobId, BlobInfo}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable

object Manager {
  def props(maxWriters: Int, prefix: String, batchSize: Int, partLen: Long, in: ZRecordReaderT): Props =
    Props(classOf[Manager], maxWriters, prefix,
      batchSize, partLen, in)
}

class Manager(maxWriters: Int, prefix: String, batchSize: Int, partLen: Long, in: ZRecordReaderT) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private val reader = context.actorOf(Reader.props(batchSize, in))
  private val writers = mutable.Set.empty[ActorRef]

  private val uri = new URI(prefix)
  private var writerId = 0
  private var isOpen = true
  private val maxLog = 42L
  private var nLog = 0L

  override def preStart(): Unit = newWriter(2)

  def receive: Receive = {
    case Available if writers.size < maxWriters =>
        newWriter()

    case Finished =>
      log.info("Reader finished reading")
      isOpen = false
      writers.foreach(_ ! Finished)

    case Terminated(writer) =>
      writers.remove(writer)
      val n = writers.size
      log.info(s"Writer terminated - $n remaining")
      if (n > 0 && isOpen) {
        newWriter()
      } else {
        log.info("stopping")
        context.stop(self)
      }

    case x =>
      if (nLog < maxLog){
        log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
        nLog += 1
      }
  }

  @tailrec
  private final def newWriter(n: Int = 1): Unit = {
    val name = f"$writerId%05d"
    val blobInfo = BlobInfo.newBuilder(BlobId.of(uri.getAuthority, uri.getPath.stripPrefix("/") + s"_$name")).build()
    val writer = context.actorOf(Writer.props(reader, blobInfo, partLen), name)
    writers += writer
    context.watch(writer)
    writerId += 1
    log.info(s"Created writer $name for $uri")
    if (n > 1) newWriter(n-1)
  }
}
