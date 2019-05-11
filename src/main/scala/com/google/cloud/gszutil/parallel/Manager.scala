package com.google.cloud.gszutil.parallel

import java.net.URI

import akka.actor.{Actor, ActorRef, Props, Terminated}
import com.google.cloud.gszutil.parallel.ActorSystem.Finished
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Manager {
  def props(nWorkers: Int, dd: String, prefix: String,
            storage: Storage, batchSize: Int, partLen: Long): Props =
    Props(classOf[Manager], nWorkers, dd, prefix,
      storage, batchSize, partLen)
}

class Manager(nWorkers: Int, dd: String, prefix: String, storage: Storage, batchSize: Int, partLen: Long) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  private val reader = context.actorOf(Reader.props(self, dd, batchSize))
  private val writers = mutable.Set.empty[ActorRef]

  private val uri = new URI(prefix)
  private var writerId = 0
  private var isOpen = true

  override def preStart(): Unit = {
    (1 until nWorkers).foreach(_ => newWriter())
  }

  def receive: Receive = {
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
      log.error(s"Unable to accept ${x.getClass.getSimpleName} message")
  }

  def newWriter(): ActorRef = {
    val name = f"$writerId%05d"
    val blobInfo = BlobInfo.newBuilder(BlobId.of(uri.getAuthority, uri.getPath.stripPrefix("/") + s"_$name")).build()
    val writer = context.actorOf(Writer.props(self, reader, storage, blobInfo, partLen), name)
    writers += writer
    context.watch(writer)
    writerId += 1
    log.info(s"Created writer $name for $uri")
    writer
  }
}
