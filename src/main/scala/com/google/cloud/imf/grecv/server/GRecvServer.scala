package com.google.cloud.imf.grecv.server

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.imf.grecv.{GRecvConfig, GzipCodec}
import com.google.cloud.imf.util.Logging
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder

class GRecvServer(cfg: GRecvConfig, creds: OAuth2Credentials) extends Logging {
  private val server: Server = {
    val ex = Executors.newWorkStealingPool()
    val b = NettyServerBuilder
      .forAddress(new InetSocketAddress(cfg.host, cfg.port))
      .addService(new GRecvService(creds))
      .compressorRegistry(GzipCodec.compressorRegistry)
      .executor(ex)
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

  def shutdown(): Unit = {
    server.shutdownNow()
  }
}
