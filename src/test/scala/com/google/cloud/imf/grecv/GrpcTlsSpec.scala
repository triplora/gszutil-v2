package com.google.cloud.imf.grecv

import com.google.cloud.imf.grecv.server.GRecvServer
import com.google.cloud.imf.util.Services

class GrpcTlsSpec extends TCPIPSpec {
  "grecv" should "grpc-tls" in {
    val cfg = GRecvConfig(Host, Port, tls = true, debug = true,
      key = "tmp/key.pem", chain = "tmp/chain.pem"
    )
    val s = new GRecvServer(cfg, Services.storage())
    s.start(block = false)
  }
}
