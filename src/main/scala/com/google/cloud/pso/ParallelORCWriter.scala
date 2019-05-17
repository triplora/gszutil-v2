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
    * @param watcher ActorRef
    */
  case class FeederArgs(in: ZRecordReaderT, batchSize: Int, uri: URI, maxBytes: Long, writerOptions: WriterOptions, nWorkers: Int, copyBook: CopyBook, watcher: ActorRef)

  /**
    *
    * @param copyBook CopyBook
    * @param writer ORC Writer
    * @param maxBytes number of bytes to accept before closing the writer
    * @param lRecl record length
    * @param blkSize block size
    * @param batchSize records per batch
    */
  case class WriterArgs(copyBook: CopyBook, writer: Writer, maxBytes: Long, lRecl: Int, blkSize: Int, batchSize: Int, path: Path)

  /** Buffer containing data for a single batch
    *
    * @param buf raw bytes from input
    * @param lRecl record size in bytes
    * @param blkSize block size in bytes (integer multiple of record size)
    * @param limit index past last byte of data
    */
  case class Batch(buf: Array[Byte], lRecl: Int, blkSize: Int, var limit: Int = 0)

  case object Finished
  case object Error

  /** Responsible for reading from input Data Set and creating ORC Writers
    * Creates writer child actors at startup
    */
  class Feeder(args: FeederArgs) extends Actor with Logging {
    private var startTime: Long = -1
    private var endTime: Long = -1
    private var lastSend: Long = -1
    private var lastRecv: Long = -1
    private var activeTime: Long = 0
    private var nSent = 0L
    private var totalBytes = 0L
    private val maxLog = 20
    private var nLog = 0
    import args._

    override def receive: Receive = {
      case x: Batch =>
        lastRecv = System.currentTimeMillis
        if (lastSend > 100L && nLog < maxLog) {
          val dt = lastRecv - lastSend
          logger.info(s"$dt ms since last send")
          nLog += 1
        }
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
        totalBytes += bb.limit
        sender ! x
        nSent += 1
        lastSend = System.currentTimeMillis
        activeTime += (lastSend - lastRecv)
        if (!in.isOpen) {
          val mbps = ((8.0d * totalBytes) / (activeTime / 1000.0)) / 1000000
          logger.info(s"Finished reading $nSent chunks with $totalBytes bytes in $activeTime ms (${f"$mbps%1.2f"} mbps)")
          context.become(finished)
        }

      case Terminated(w) =>
        writers.remove(w)
        newPart()

      case _ =>
    }

    def finished: Receive = {
      case _: Batch =>
        context.stop(sender)

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
      val args = WriterArgs(copyBook, writer, maxBytes, in.lRecl, in.blkSize, batchSize, path)
      val w = context.actorOf(Props(classOf[OrcWriter], args), s"OrcWriter-$partName")
      context.watch(w)
      writers.add(w)
      partId += 1
    }

    override def preStart(): Unit = {
      if (in.lRecl != copyBook.lRecl) {
        logger.error("input lRecl != copybook lRecl")
        in.close()
        watcher ! Error
      } else {
        startTime = System.currentTimeMillis
        for (_ <- 0 until nWorkers)
          newPart()
      }
    }

    override def postStop(): Unit = {
      endTime = System.currentTimeMillis
      val totalTime = endTime - startTime
      val mbps = ((8.0d * totalBytes) / (totalTime / 1000.0d)) / 1000000.0d
      val v = f"$mbps%1.2f"
      val wait = totalTime - activeTime
      logger.info(s"Finished writing $totalBytes bytes; $nSent chunks; $totalTime ms; $v mbps; active $activeTime ms; wait $wait ms")
      watcher ! Finished
    }
  }


  /** Responsible for writing a single output partition
    */
  class OrcWriter(args: WriterArgs) extends Actor {
    import args._
    private val reader = copyBook.reader
    private var nBytes: Long = 0
    private var elapsedTime: Long = 0
    private var startTime: Long = -1
    private var endTime: Long = -1

    override def preStart(): Unit = {
      context.parent ! Batch(new Array[Byte](lRecl * batchSize), lRecl, blkSize)
      startTime = System.currentTimeMillis()
      logger.info(s"Starting writer for ${args.path}")
    }

    override def receive: Receive = {
      case x: Batch =>
        nBytes += x.limit
        val t0 = System.currentTimeMillis
        reader
          .readOrc(new ZDataSet(x.buf, x.lRecl, x.blkSize, x.limit))
          .filter(_.size > 0)
          .foreach(writer.addRowBatch)
        val t1 = System.currentTimeMillis
        elapsedTime += (t1 - t0)
        if (nBytes < maxBytes)
          sender ! x
        else context.stop(self)
      case _ =>
    }

    override def postStop(): Unit = {
      val t0 = System.currentTimeMillis
      writer.close()
      val t1 = System.currentTimeMillis
      elapsedTime += (t1 - t0)
      endTime = System.currentTimeMillis
      val dt = endTime - startTime
      val idle = dt - elapsedTime
      val mbps = ((8.0d * nBytes) / (elapsedTime / 1000.0)) / 1000000
      val v = f"$mbps%1.2f"
      logger.info(s"Stopping writer for ${args.path} after writing $nBytes bytes in $elapsedTime ms ($v mbps) $dt ms total $idle ms idle")
    }
  }

  def run(prefix: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          writerOptions: WriterOptions,
          maxWriters: Int,
          batchSize: Int = 1024,
          partLen: Long = 128 * 1024 * 1024,
          timeoutMinutes: Int = 30): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of())
    val sys = ActorSystem("gsz", conf)
    val inbox = Inbox.create(sys)
    val args = FeederArgs(in, batchSize, new URI(prefix), partLen, writerOptions, maxWriters, copyBook, inbox.getRef())
    sys.actorOf(Props(classOf[Feeder], args))
    inbox.receive(FiniteDuration(timeoutMinutes, MINUTES)) match {
      case Finished =>
        logger.info("terminating actor system")
        sys.terminate()

      case Error =>
        logger.info("terminating actor system")
        sys.terminate()
        System.exit(1)

      case x =>
        logger.info(s"received unexpected $x")
    }
  }
}
