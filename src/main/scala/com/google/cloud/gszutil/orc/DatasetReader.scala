package com.google.cloud.gszutil.orc

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef, EscalatingSupervisorStrategy, Props, SupervisorStrategy, Terminated}
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/** Responsible for reading from input Data Set and creating ORC Writers
  * Creates writer child actors at startup
  */
class DatasetReader(args: DatasetReaderArgs) extends Actor with Logging {
  private var startTime: Long = -1
  private var endTime: Long = -1
  private var lastSend: Long = -1
  private var lastRecv: Long = -1
  private var activeTime: Long = 0
  private var nSent = 0L
  private var totalBytes = 0L
  import args._

  override def preStart(): Unit = {
    if (lRecl != copyBook.LRECL) {
      logger.error("input lRecl != copybook lRecl")
      in.close()
    } else {
      startTime = System.currentTimeMillis
      for (_ <- 0 until nWorkers)
        newPart()
    }
  }

  override def receive: Receive = {
    case bb: ByteBuffer =>
      lastRecv = System.currentTimeMillis

      while (bb.hasRemaining && in.isOpen) {
        if (in.read(bb) < 0) in.close()
      }

      totalBytes += bb.limit
      bb.flip()
      sender ! bb
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
    case _: ByteBuffer =>
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
    val args = ORCFileWriterArgs(copyBook, maxBytes, batchSize, path, gcs)
    val w = context.actorOf(Props(classOf[ORCFileWriter], args), s"OrcWriter-$partName")
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
