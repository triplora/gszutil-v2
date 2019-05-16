package com.google.cloud.pso

import java.net.URI
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, Inbox, PoisonPill, Props, Terminated}
import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZDataSet, ZRecordReaderT}
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.{OrcFile, Writer}

import scala.collection.mutable

object ParallelORCWriter extends Logging {

  /**
    *
    * @param in input Data Set
    * @param batchSize rows per batch
    * @param uri prefix URI
    * @param maxBytes input bytes per part
    * @param writerOptions OrcFile.WriterOptions
    * @param nWorkers worker count
    * @param copyBook CopyBook
    */
  case class FeederArgs(in: ZRecordReaderT, batchSize: Int, uri: URI, maxBytes: Long, writerOptions: WriterOptions, nWorkers: Int, copyBook: CopyBook)

  /**
    *
    * @param copyBook CopyBook
    * @param writer ORC Writer
    * @param maxBytes number of bytes to accept before closing the writer
    * @param lRecl record length
    * @param blkSize block size
    * @param batchSize records per batch
    */
  case class WriterArgs(copyBook: CopyBook, writer: Writer, maxBytes: Long, lRecl: Int, blkSize: Int, batchSize: Int)

  /** Buffer containing data for a single batch
    *
    * @param buf raw bytes from input
    * @param lRecl record size in bytes
    * @param blkSize block size in bytes (integer multiple of record size)
    * @param limit index past last byte of data
    */
  case class Batch(buf: Array[Byte], lRecl: Int, blkSize: Int, var limit: Int = 0)

  /** Responsible for reading from input Data Set and creating ORC Writers
    * Creates writer child actors at startup
    */
  class Feeder(args: FeederArgs) extends Actor with Logging {
    import args._
    override def receive: Receive = {
      case x: Batch =>
        val buf = x.buf
        val bb = ByteBuffer.wrap(buf)

        // fill buffer
        while (bb.hasRemaining && in.isOpen) {
          val n = in.read(buf, bb.position, bb.remaining)
          if (n < 0) {
            bb.limit(bb.position)
            in.close()
          } else {
            val newPosition = bb.position + n
            bb.position(newPosition)
          }
        }
        x.limit = bb.limit
        sender ! x
        if (!in.isOpen)
          context.become(finished)

      case Terminated(w) =>
        writers.remove(w)
        newPart()

      case _ =>
    }

    def finished: Receive = {
      case _: Batch =>
        sender ! PoisonPill

      case Terminated(w) =>
        writers.remove(w)
        if (writers.isEmpty)
          context.stop(self)

      case _ =>
    }

    private var partId = 0
    private val writers = mutable.Set.empty[ActorRef]

    private def newPart(): Unit = {
      val partName = f"$partId%05d"
      val path = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/") + s"/part-$partName.orc"}")
      val writer = OrcFile.createWriter(path, writerOptions)
      val w = context.actorOf(Props(classOf[OrcWriter], WriterArgs(copyBook, writer, maxBytes, in.lRecl, in.blkSize, batchSize)), s"OrcWriter-$partName")
      context.watch(w)
      writers.add(w)
      partId += 1
    }

    override def preStart(): Unit = {
      for (_ <- 0 until nWorkers)
        newPart()
    }
  }


  /** Responsible for writing a single output partition
    */
  class OrcWriter(args: WriterArgs) extends Actor {
    import args._
    private val reader = copyBook.reader
    private var nBytes: Long = 0

    override def preStart(): Unit =
      context.parent ! Batch(new Array[Byte](lRecl * batchSize), lRecl, blkSize)

    override def receive: Receive = {
      case x: Batch =>
        nBytes += x.limit
        reader
          .readOrc(new ZDataSet(x.buf, x.lRecl, x.blkSize, x.limit))
          .filter(_.size > 0)
          .foreach(writer.addRowBatch)
        if (nBytes < maxBytes)
          sender ! x
        else
          context.stop(self)
      case _ =>
    }

    override def postStop(): Unit =
      writer.close()
  }

  def run(prefix: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          writerOptions: WriterOptions,
          maxWriters: Int,
          batchSize: Int = 1024,
          partLen: Long = 64 * 1024 * 1024,
          timeoutMinutes: Int = 30): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of())
    val sys = ActorSystem("gsz", conf)
    val inbox = Inbox.create(sys)
    val feeder = sys.actorOf(Props(classOf[Feeder], FeederArgs(in, batchSize, new URI(prefix), partLen, writerOptions, maxWriters, copyBook)))
    inbox.watch(feeder)
    inbox.receive(FiniteDuration(timeoutMinutes, MINUTES)) match {
      case Terminated(_) =>
        logger.info("terminating actor system")
        sys.terminate()
      case _ =>
    }
  }
}
