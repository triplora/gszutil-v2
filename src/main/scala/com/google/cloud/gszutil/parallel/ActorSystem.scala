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
  case class SendData(worker: ActorRef) extends Message
  case class Batch(data: Array[Byte], limit: Int, partId: Int) extends Message
  case object Finished extends Message

  def start(prefix: String, storage: Storage,
            nWorkers: Int = 20,
            dd: String = "INFILE",
            batchSize: Long = 80000000): Unit = {
    val inbox = Inbox.create(sys)
    val master = sys.actorOf(Master.props(nWorkers, dd, prefix, storage, batchSize))
    inbox.watch(master)
    inbox.receive(FiniteDuration.apply(30, TimeUnit.MINUTES)) match {
      case Terminated =>
        sys.terminate()
      case _ =>
    }
  }

  object Reader {
    def props(a: ActorRef, dd: String): Props = Props(classOf[Reader], a, dd)
  }

  class Reader(val master: ActorRef, val dd: String) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)
    private lazy val in: TRecordReader = ZOS.readDD(dd)
    private var hasRemaining = true
    private var partId: Int = -1
    override def receive: Receive = {
      case SendData(worker) =>
        partId += 1
        val data = new Array[Byte](in.lRecl * 1024)

        val buf = ByteBuffer.wrap(data)
        while (buf.hasRemaining && hasRemaining){
          val n = in.read(data, buf.position, buf.remaining)
          if (n < 0) {
            hasRemaining = false
            in.close()
          } else {
            val newPosition = buf.position + n
            buf.position(newPosition)
          }
        }
        if (buf.position > 0)
          worker ! Batch(data, buf.limit, partId)

        if (!hasRemaining)
          master ! Finished
        else
          master ! Available

      case _ =>
    }
  }

  object Master{
    def props(nWorkers: Int, dd: String, prefix: String, storage: Storage, batchSize: Long): Props = Props(classOf[Master], nWorkers, dd, prefix, storage, batchSize)
  }

  class Master(nWorkers: Int, dd: String, prefix: String, storage: Storage, batchSize: Long) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)
    private val workers: mutable.Set[ActorRef] = mutable.Set.empty
    private val reader: ActorRef = sys.actorOf(Reader.props(self, dd))

    for (i <- 1 to nWorkers){
      val id = f"$i%02d"
      val worker = sys.actorOf(Worker.props(self, storage, prefix, i, batchSize), s"worker$id")
      workers += worker
      context.watch(worker)
      reader ! SendData(worker)
    }

    def receive: Receive = {
      case Terminated(worker) =>
        workers.remove(worker)
        if (workers.isEmpty) self ! PoisonPill

      case Finished =>
        sender() ! PoisonPill
        for (worker <- workers) worker ! Finished

      case Available =>
        reader ! SendData(sender())

      case _ =>
    }
  }

  object Worker {
    def props(parent: ActorRef, storage: Storage, prefix: String, id: Int, batchSize: Long): Props =
      Props(classOf[Worker], parent, storage, prefix, id, batchSize)
  }

  class Worker(private val storage: Storage, parent: ActorRef, val prefix: String, id: Int, batchSize: Long) extends Actor {
    private val log = LoggerFactory.getLogger(getClass)

    def getSuffix(j: Int): String = f"$id%02d" + "-" + f"$j%02d"

    def receive: Receive = {
      case batch: Batch =>
        val suffix = getSuffix(0)
        val uri = new URI(prefix + suffix)
        val blobInfo = BlobInfo
          .newBuilder(BlobId.of(uri.getAuthority, uri.getPath))
          .build()
        val out = storage.writer(blobInfo)
        context.become(write(out, 0, 0))
        self ! batch

      case _ =>
    }

    def write(out: WriteChannel, j: Int, n: Long): Receive = {
      case Batch(data, limit, _) =>
        val suffix = getSuffix(j)

        val buf = ByteBuffer.wrap(data)
        buf.limit(limit)
        while (buf.hasRemaining)
          out.write(buf)

        if (n > batchSize) {
          out.close()
          log.info(s"${self.path} Finished writing $limit bytes")
          val uri = new URI(prefix + suffix)
          val blobInfo = BlobInfo
            .newBuilder(BlobId.of(uri.getAuthority, uri.getPath))
            .build()
          log.info(s"${self.path} Writing to $uri")
          val next = storage.writer(blobInfo)
          context.become(write(next, j + 1, n + limit))
        }

        parent ! Available

      case Finished =>
        out.close()
        self ! PoisonPill

      case _ =>
    }
  }
}
