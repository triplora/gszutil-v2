package com.google.cloud.imf.grecv

import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest

trait Uploader {
  def upload(req: GRecvRequest,
             host: String,
             port: Int,
             nConnections: Int,
             in: ZRecordReaderT): Result
}
