package com.google.cloud.imf.grecv

import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest

trait Receiver {
  def run(cfg: GRecvConfig): Result
  def recv(req: GRecvRequest,
           host: String,
           port: Int,
           nConnections: Int,
           parallelism: Int,
           tls: Boolean,
           in: ZRecordReaderT): Result
}
