package com.google.cloud.imf.grecv.grpc

import java.util.concurrent.TimeUnit

import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.pb.GRecvGrpc
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, GRecvResponse}
import com.google.cloud.imf.util.Logging
import com.google.common.util.concurrent.MoreExecutors
import io.grpc.Channel

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class Client(ch: Channel) extends Logging {
  private val asyncStub = GRecvGrpc.newStub(ch)
    .withCompression("gzip")

  def send(request: GRecvRequest,
           in: ZRecordReaderT,
           connections: Int = 1): Seq[(GRecvResponse, String)] = {
    logger.debug(s"sending ${in.getDsn}")

    val streams = (0 until connections).map{i =>
      val s = WriteStream(asyncStub, i.toString)
      s.init(request)
      s
    }

    val blksz = in.lRecl * 1024
    val lrecl = in.lRecl
    val buf = new Array[Byte](blksz)
    var count = 0L
    var bytesRead = 0L
    var sid = 0
    var n = 0
    var pos = 0
    var stream = streams(sid)

    while (n > -1){
      pos = 0
      while (n > -1 && blksz - pos >= lrecl){
        n = in.read(buf, pos, lrecl)
        if (n > 0) {
          bytesRead += n
          pos += n
        }
      }
      if (pos > 0) {
        count += 1
        stream.onNext(buf,0,pos)
        if (connections > 1){
          sid += 1
          if (sid >= connections) sid = 0
          stream = streams(sid)
        }
      }
    }
    logger.debug("end of stream")
    streams.foreach(_.onCompleted())
    logger.debug("waiting for responses")

    implicit val ec = ExecutionContext.fromExecutor(MoreExecutors.directExecutor())

    val responses = Await.result(
      Future.sequence(streams.map(_.response.future)),
      Duration(1, TimeUnit.HOURS))
    logger.debug(s"received ${responses.length} responses")
    responses.zip(streams.map(_.result))
  }
}
