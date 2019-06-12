package com.google.cloud.pso

import java.net.URI
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, EscalatingSupervisorStrategy, Props, SupervisorStrategy, Terminated}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZDataSet, ZRecordReaderT}
import com.google.cloud.gszutil.{CopyBook, Util}
import com.google.cloud.storage.Storage
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path, SimpleGCSFileSystem}
import org.apache.orc.{CompressionKind, NoOpMemoryManager, OrcFile, Writer}

import scala.collection.mutable
import scala.concurrent.Await

object ParallelORCWriter extends Logging {

  /**
    *
    * @param in input Data Set
    * @param batchSize rows per batch
    * @param uri prefix URI
    * @param maxBytes input bytes per part
    * @param nWorkers worker count
    * @param copyBook CopyBook
    */
  case class FeederArgs(in: ZRecordReaderT, batchSize: Int, uri: URI, maxBytes: Long, nWorkers: Int, copyBook: CopyBook, gcs: Storage)

  /**
    *
    * @param copyBook CopyBook
    * @param maxBytes number of bytes to accept before closing the writer
    * @param blkSize block size
    * @param batchSize records per batch
    */
  case class WriterArgs(copyBook: CopyBook, maxBytes: Long, blkSize: Int, batchSize: Int, path: Path, gcs: Storage)

  /** Buffer containing data for a single batch
    *
    * @param buf raw bytes from input
    * @param limit index past last byte of data
    */
  case class Batch(buf: Array[Byte], var limit: Int = 0)

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
    private val r = Runtime.getRuntime
    import args._

    override def preStart(): Unit = {
      if (in.lRecl != copyBook.lRecl) {
        logger.error("input lRecl != copybook lRecl")
        in.close()
      } else {
        startTime = System.currentTimeMillis
        for (_ <- 0 until nWorkers)
          newPart()
      }
    }

    def logMem(): String = {
      val free = r.freeMemory() * 1.0d / 1e9
      val total = r.totalMemory() * 1.0d / 1e9
      val max = r.maxMemory() * 1.0d / 1e9
      s"Memory: $free of $total total ($max max)"
    }

    override def receive: Receive = {
      case x: Batch =>
        lastRecv = System.currentTimeMillis
        val dt = lastRecv - lastSend
        if (dt > 200L && lastSend > 100L && nLog < maxLog) {
          logger.info(s"$dt ms since last send " + logMem())
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
        if (nLog < maxLog){
          logger.info(s"sent batch with limit=${x.limit} ${logMem()}")
          nLog += 1
        }
        nSent += 1
        lastSend = System.currentTimeMillis
        activeTime += (lastSend - lastRecv)
        if (!in.isOpen) {
          val mbps = Util.fmbps(totalBytes,activeTime)
          logger.info(s"Finished reading $nSent chunks with $totalBytes bytes in $activeTime ms ($mbps mbps)")
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
      val args = WriterArgs(copyBook, maxBytes, in.blkSize, batchSize, path, gcs)
      val w = context.actorOf(Props(classOf[OrcWriter], args), s"OrcWriter-$partName")
      context.watch(w)
      writers.add(w)
      partId += 1
    }

    override def postStop(): Unit = {
      endTime = System.currentTimeMillis
      val totalTime = endTime - startTime
      val mbps = Util.fmbps(totalBytes, totalTime)
      val wait = totalTime - activeTime
      logger.info(s"Finished writing $totalBytes bytes; $nSent chunks; $totalTime ms; $mbps mbps; active $activeTime ms; wait $wait ms")
      context.system.terminate()
    }

    override def supervisorStrategy: SupervisorStrategy = new EscalatingSupervisorStrategy().create()
  }

  // TODO collect heartbeat and print cumulative stats
  class HeartBeatActor extends Actor {
    override def receive: Receive = {
      case _ =>

    }
  }

  case class HeartBeat()


  /** Responsible for writing a single output partition
    */
  class OrcWriter(args: WriterArgs) extends Actor {
    import args._
    private val reader = copyBook.reader
    private var bytesIn: Long = 0
    private var elapsedTime: Long = 0
    private var startTime: Long = -1
    private var endTime: Long = -1
    private var writer: Writer = _
    private val stats = new FileSystem.Statistics(SimpleGCSFileSystem.Scheme)
    private val r = Runtime.getRuntime
    private var i: Long = 0
    private val n: Long = 100

    override def preStart(): Unit = {
      val writerOptions = OrcFile
        .writerOptions(SimpleORCWriter.configuration())
        .setSchema(copyBook.getOrcSchema)
        .memory(NoOpMemoryManager)
        .compress(CompressionKind.ZLIB)
        .fileSystem(new SimpleGCSFileSystem(gcs, stats))
      writer = OrcFile.createWriter(path, writerOptions)
      context.parent ! Batch(new Array[Byte](copyBook.lRecl * batchSize))
      startTime = System.currentTimeMillis()
      logger.info(s"Starting writer for ${args.path}")
    }

    def logMem(): String = {
      val free = r.freeMemory()
      val total = r.totalMemory()
      val max = r.maxMemory()
      s"Memory: $free of $total total ($max max)"
    }

    override def receive: Receive = {
      case x: Batch =>
        val shouldLog = i < n
        if (shouldLog) {
          logger.info(s"received batch $i ${x.buf.length} ${x.limit} "+ logMem())
        }
        bytesIn += x.limit
        val t0 = System.currentTimeMillis
        reader
          .readOrc(new ZDataSet(x.buf, copyBook.lRecl, blkSize, x.limit))
          .filter(_.size > 0)
          .foreach(writer.addRowBatch)
        if (shouldLog) {
          logger.info(s"finished batch $i " + logMem())
          i += 1
        }
        val t1 = System.currentTimeMillis
        elapsedTime += (t1 - t0)
        if (stats.getBytesWritten < maxBytes)
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
      val bytesOut = stats.getBytesWritten
      val ratio = (bytesOut * 1.0d) / bytesIn
      val mbps = Util.fmbps(bytesOut, elapsedTime)
      logger.info(s"Stopping writer for ${args.path} after writing $bytesOut bytes in $elapsedTime ms ($mbps mbps) $dt ms total $idle ms idle $bytesIn bytes read ${f"$ratio%1.2f"} compression ratio")
    }
  }

  def run(gcsUri: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          gcs: Storage,
          maxWriters: Int = 5,
          batchSize: Int = 1024,
          partLen: Long = 128 * 1024 * 1024,
          timeoutMinutes: Int = 60): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gsz", conf)
    sys.actorOf(Props(classOf[Feeder], FeederArgs(in, batchSize, new URI(gcsUri), partLen, maxWriters, copyBook, gcs)), "ZReader")
    Await.result(sys.whenTerminated, atMost = FiniteDuration(timeoutMinutes, MINUTES))
  }
}
