package com.google.cloud.imf.grecv

import com.google.cloud.bqsh.GCS
import com.google.cloud.imf.grecv.grpc.Service

class GrpcTlsSpec extends TCPIPSpec {
  "grecv" should "grpc-tls" in {
    val cfg = GRecvConfig(Host, Port, tls = true, debug = true,
      key = "tmp/key.pem", chain = "tmp/chain.pem"
    )
    val s = new Service(cfg, GCS.getDefaultClient())
    s.start(block = false)
  }
}
