package com.google.cloud.imf.grecv.socket

import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.grecv.{GRecvConfig, Receiver}
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.Logging

import scala.util.{Failure, Success}

object SocketReceiver extends Receiver with Logging {
  override def run(cfg: GRecvConfig): Result = {
    import scala.concurrent.ExecutionContext.Implicits.global
    try {
      val server = SocketUtil.listen(cfg.host, cfg.port, cfg.tls,
        chain = cfg.chain, key = cfg.key)
      SocketServer.serverBegin(server)
      Result.Success
    } catch {
      case t: Throwable =>
        Result.Failure(t.getMessage)
    }
  }

  override def recv(req: GRecvProto.GRecvRequest,
                    host: String,
                    port: Int,
                    nConnections: Int,
                    parallelism: Int,
                    tls: Boolean,
                    in: ZRecordReaderT): Result = {
    SocketClient.clientBegin(in = in,
      request = req,
      nConnections = nConnections,
      host = host,
      port = port,
      tls = tls) match {
      case Failure(e) =>
        logger.error("Dataset Upload Failed")
        Result.Failure(e.getMessage)
      case Success(r) =>
        val msg = s"""return code: ${r.rc}
                     |bytes in: ${r.bytesIn}
                     |msgCount: ${r.msgCount}""".stripMargin
        logger.info(msg)
        if (r.rc == 0) {
          logger.info("Dataset Upload Complete")
          Result.Success
        } else {
          Result.Failure("upload failed")
        }
    }
  }
}
