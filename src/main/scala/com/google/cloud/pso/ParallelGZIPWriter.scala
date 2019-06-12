package com.google.cloud.pso

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, EscalatingSupervisorStrategy, Props, SupervisorStrategy, Terminated}
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.{GZIPChannel, Logging}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.Await

object ParallelGZIPWriter extends Logging {

  /**
    *
    * @param in input Data Set
    * @param batchSize rows per batch
    * @param bucket prefix URI
    * @param objName prefix URI
    * @param maxBytes input bytes per part
    * @param nWorkers worker count
    */
  case class FeederArgs(in: ZRecordReaderT, batchSize: Int, bucket: String, objName: String, maxBytes: Long, nWorkers: Int, gcs: Storage)

  /**
    *
    * @param maxBytes number of bytes to accept before closing the writer
    * @param batchLen bytes per batch (lRecl * batchSize)
    */
  case class WriterArgs(maxBytes: Long, batchLen: Int, path: BlobId, gcs: Storage)

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
    import args._

    override def preStart(): Unit = {
      startTime = System.currentTimeMillis
      for (_ <- 0 until nWorkers)
        newPart()
    }

    override def receive: Receive = {
      case x: Batch =>
        lastRecv = System.currentTimeMillis
        val dt = lastRecv - lastSend
        if (dt > 200L && lastSend > 100L && nLog < maxLog) {
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
      val path = BlobId.of(bucket, objName.stripPrefix("/") + s"/part-$partName.gz")
      val args = WriterArgs(maxBytes, in.lRecl * batchSize, path, gcs)
      val w = context.actorOf(Props(classOf[GZIPWriter], args), s"GZIPWriter-$partName")
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


  /** Responsible for writing a single output partition
    */
  class GZIPWriter(args: WriterArgs) extends Actor with Logging {
    import args._
    private var bytesIn: Long = 0
    private var elapsedTime: Long = 0
    private var startTime: Long = -1
    private var endTime: Long = -1
    private lazy val writer: GZIPChannel = {
      logger.info(s"opening channel gs://${path.getBucket}/${path.getName}")
      val w = gcs.writer(BlobInfo.newBuilder(path).build())
      new GZIPChannel(w, 65536)
    }

    override def preStart(): Unit = {
      context.parent ! Batch(new Array[Byte](batchLen))
      startTime = System.currentTimeMillis()
      logger.info(s"Starting writer for ${args.path}")
    }

    override def receive: Receive = {
      case x: Batch =>
        bytesIn += x.limit
        val t0 = System.currentTimeMillis
        writer.write(ByteBuffer.wrap(x.buf))
        val t1 = System.currentTimeMillis
        elapsedTime += (t1 - t0)
        if (writer.getBytesWritten < maxBytes)
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
      val bytesOut = writer.getBytesWritten
      val ratio = (bytesOut * 1.0d) / bytesIn
      val mbps = Util.fmbps(bytesOut, elapsedTime)
      logger.info(s"Stopping writer for ${args.path} after writing $bytesOut bytes in $elapsedTime ms ($mbps mbps) $dt ms total $idle ms idle $bytesIn bytes read ${f"$ratio%1.2f"} compression ratio")
    }
  }

  def run(bucket: String,
          prefix: String,
          in: ZRecordReaderT,
          gcs: Storage,
          maxWriters: Int = 5,
          batchSize: Int = 1024,
          partLen: Long = 128 * 1024 * 1024,
          timeoutMinutes: Int = 60): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gsz", conf)
    sys.actorOf(Props(classOf[Feeder], FeederArgs(in, batchSize, bucket, prefix, partLen, maxWriters, gcs)), "ZReader")
    Await.result(sys.whenTerminated, atMost = FiniteDuration(timeoutMinutes, MINUTES))
  }
}
