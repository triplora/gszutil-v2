package com.google.cloud.gszutil.parallel


import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Inbox, PoisonPill, Props, Terminated}
import com.google.cloud.WriteChannel
import com.google.cloud.gszutil.ZOS
import com.google.cloud.gszutil.io.TRecordReader
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object ActorSystem {

  private val sys = akka.actor.ActorSystem("gszutil")
  sealed trait Message
  case object Available extends Message
  case class Read(dd: String) extends Message
  case class Batch(data: Array[Byte], limit: Int, partId: Int) extends Message
  case object Finished extends Message

  def start(prefix: String,
            storage: Storage,
            nWorkers: Int = 20,
            dd: String = "INFILE",
            batchSize: Int = 1024,
            partLen: Long = 80000000): Unit = {
    val inbox = Inbox.create(sys)
    val master = sys.actorOf(masterProps(nWorkers, dd, prefix, storage, batchSize, partLen))
    inbox.watch(master)
    inbox.receive(FiniteDuration(30, TimeUnit.MINUTES)) match {
      case Terminated =>
        sys.terminate()
      case _ =>
    }
  }

  def readerProps(master: ActorRef, dd: String, batchSize: Int): Props =
    Props(classOf[Reader], master, dd, batchSize)

  def masterProps(nWorkers: Int, dd: String, prefix: String,
                  storage: Storage, batchSize: Int, partLen: Long): Props =
    Props(classOf[Master], nWorkers, dd, prefix,
      storage, batchSize, partLen)

  def writerProps(parent: ActorRef,
                  reader: ActorRef,
                  storage: Storage,
                  blobInfo: BlobInfo,
                  partLength: Long): Props =
    Props(classOf[Writer], parent, reader, storage, blobInfo, partLength)

  def getSuffix(prefix: String, workerId: Int): String =
    prefix + "_" + f"$workerId%05d"

  def getUri(blobInfo: BlobInfo): String =
    s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}"

  def prepareBatch(in: TRecordReader, batchSize: Int, batchId: Int): Batch = {
    val data = new Array[Byte](in.lRecl * batchSize)
    val buf = ByteBuffer.wrap(data)
    while (buf.hasRemaining && in.isOpen){
      val n = in.read(data, buf.position, buf.remaining)
      if (n < 0) {
        in.close()
      } else {
        val newPosition = buf.position + n
        buf.position(newPosition)
      }
    }
    Batch(data, buf.position, batchId)
  }

  class Reader(master: ActorRef, dd: String, batchSize: Int) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)
    private lazy val in: TRecordReader = ZOS.readDD(dd)
    private var partId: Int = -1

    override def receive: Receive = {
      case Available if in.isOpen =>
        partId += 1
        val batch = prepareBatch(in, batchSize, partId)
        if (batch.limit > 0)
          sender() ! batch

        if (in.isOpen)
          master ! Available
        else {
          context.become(finished)
          master ! Finished
        }

      case x =>
        throw new IllegalArgumentException(s"Unable to accept ${x.getClass.getSimpleName} message")
    }

    def finished: Receive = {
      case x =>
        throw new IllegalArgumentException(s"Unable to accept ${x.getClass.getSimpleName} message in finished state")
    }
  }

  class Master(nWorkers: Int, dd: String, prefix: String, storage: Storage, batchSize: Int, partLen: Long) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)
    private val writers: mutable.Set[ActorRef] = mutable.Set.empty
    private val reader: ActorRef = context.actorOf(readerProps(self, dd, batchSize))

    private val uri = new URI(prefix)

    private var workerId = 0
    private var isOpen = true

    def newWorker(): ActorRef = {
      val idCode = f"$workerId%05d"
      val blobInfo = BlobInfo.newBuilder(BlobId.of(uri.getAuthority, uri.getPath + s"_$idCode")).build()
      val writer = context.actorOf(writerProps(self, reader, storage, blobInfo, partLen), s"worker_" + idCode)
      writers += writer
      context.watch(writer)
      workerId += 1
      writer
    }

    (1 until nWorkers).foreach(_ => newWorker())

    def receive: Receive = {
      case Available => // Reader is available
        newWorker()

      case Finished => // Reader finished reading
        isOpen = false
        sender() ! PoisonPill
        writers.foreach(_ ! Finished)

      case Terminated(writer) =>
        writers.remove(writer)
        if (writers.isEmpty && !isOpen)
          self ! PoisonPill

      case x =>
        throw new IllegalArgumentException(s"Unable to accept ${x.getClass.getSimpleName} message")
    }
  }

  class Writer(parent: ActorRef,
               reader: ActorRef,
               storage: Storage,
               blobInfo: BlobInfo,
               partLength: Long) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)
    private var n = 0 // Stop writing when n >= partLength

    reader ! Available

    def receive: Receive = {
      case batch: Batch =>
        val out = storage.writer(blobInfo)
        log.info(s"Started writing to ${getUri(blobInfo)}")
        write(out, batch)
        context.become(writing(out))

      case x =>
        throw new IllegalArgumentException(s"Unable to accept ${x.getClass.getSimpleName} message")
    }

    def writing(out: WriteChannel): Receive = {
      case batch: Batch =>
        write(out, batch)
        if (n < partLength)
          parent ! Available
        else
          finish(out)

      case Finished => // Reader has no more bytes
        finish(out)

      case x =>
        throw new IllegalArgumentException(s"Unable to accept ${x.getClass.getSimpleName} message in writing state")
    }

    def finished: Receive = {
      case x =>
        throw new RuntimeException(s"Unable to accept ${x.getClass.getSimpleName} message in finished state")
    }

    def write(out: WriteChannel, batch: Batch): Unit = {
      val buf = ByteBuffer.wrap(batch.data)
      buf.limit(batch.limit)
      while (buf.hasRemaining)
        out.write(buf)
      n += batch.limit
    }

    def finish(out: WriteChannel): Unit = {
      out.close()
      log.info(s"${self.path} Finished writing $n bytes to ${getUri(blobInfo)}")
      context.become(finished)
      self ! PoisonPill
    }
  }
}
