package com.google.cloud.imf.grecv

import java.util.concurrent.TimeUnit

import com.google.cloud.imf.grecv.grpc.{Client, GzipCodec, MultiChannel, Service}
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse
import com.google.cloud.imf.util.{Services, TLSUtil}
import io.grpc.CompressorRegistry
import io.grpc.netty.NettyChannelBuilder

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class GrpcSpec extends TCPIPSpec {
  def server(cfg: GRecvConfig): Future[Service] = Future{
    val s = new Service(cfg, Services.storage())
    s.start(block = false)
    s
  }

  def client(cfg: GRecvConfig): Future[Seq[(GRecvResponse,String)]] = Future{
    val cr = CompressorRegistry.newEmptyInstance
    cr.register(new GzipCodec())
    val cb = NettyChannelBuilder.forAddress(cfg.host, cfg.port)
      .compressorRegistry(cr)
      .keepAliveTime(10, TimeUnit.MINUTES)

    if (cfg.tls) {
      logger.info("Configuring Client TLS")
      TLSUtil.addConscrypt(cb)
    } else {
      logger.warn("Configuring Client Plaintext")
      cb.usePlaintext()
    }

    val ch = new MultiChannel(cb, 4)
    val client = new Client(ch)

    client.send(request.build, in,2)
  }

  "grecv" should "grpc-tls" in {
    val cfg = GRecvConfig(Host, Port, tls = true, debug = true, key = "")
    val r = server(cfg)
    val w = client(cfg)
    val response2 = Await.result(w, Duration.Inf)

    response2.map{r =>
      logger.debug(s"got response ${r._1.getStatus} ${r._1.getHash} ${r._2}")
      assert(r._1.getHash == r._2, s"hash mismatch ${r._1.getHash} != ${r._2}")
    }
  }
}
