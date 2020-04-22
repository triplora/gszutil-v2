package com.google.cloud.imf.grecv.grpc

import java.net.InetSocketAddress
import java.nio.file.Paths
import java.util.concurrent.Executors

import com.google.cloud.imf.grecv.GRecvConfig
import com.google.cloud.imf.util.{Logging, SecurityUtils, TLSUtil}
import com.google.cloud.storage.Storage
import io.grpc.netty.NettyServerBuilder
import io.grpc.{CompressorRegistry, Server}

class Service(cfg: GRecvConfig, gcs: Storage) extends Logging {
  private val server: Server = {
    val ex = Executors.newWorkStealingPool()
    val cr = CompressorRegistry.newEmptyInstance
    cr.register(new GzipCodec())
    val b = NettyServerBuilder
      .forAddress(new InetSocketAddress(cfg.host, cfg.port))
      .addService(new GRecvImpl(gcs))
      .compressorRegistry(cr)
      .executor(ex)

    if (!cfg.tls) {
      logger.warn("Configuring Server with Plaintext")
    } else {
      val (key,chain) =
        if (cfg.key.isEmpty || cfg.chain.isEmpty){
          logger.debug("generating self signed certificate")
          TLSUtil.selfSigned
        } else {
          logger.debug("reading private key and certificate chain")
          val privateKey = SecurityUtils.readPrivateKey(Paths.get(cfg.key))
          val certChain = SecurityUtils.readCertificates(Paths.get(cfg.chain))
          (privateKey, certChain)
        }
      logger.debug("Configuring Server TLS")
      TLSUtil.printChain(chain)
      TLSUtil.addConscryptServer(b,key,chain)
      logger.debug("Done")
    }
    b.build
  }

  def start(block: Boolean = true): Unit = {
    logger.info(s"starting server on ${cfg.host}:${cfg.port}")
    server.start()
    if (block) {
      logger.info("awaiting server termination")
      server.awaitTermination()
      logger.info("server terminated")
    }
  }
}
