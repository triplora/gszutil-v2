package com.google.cloud.imf.grecv.grpc

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.cloud.imf.util.Logging
import com.google.common.primitives.UnsignedInts
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{CallOptions, ClientCall, ManagedChannel, MethodDescriptor}

// z/OS sockets are limited to ~10MB/s so we use multiple
class MultiChannel(b: NettyChannelBuilder, nConnections: Int)
  extends ManagedChannel with Logging {
  private val channels: Array[ManagedChannel] = (0 until nConnections).map(_ => b.build()).toArray

  private val chId = new AtomicInteger

  def newCall[ReqT, RespT](m: MethodDescriptor[ReqT, RespT], o: CallOptions): ClientCall[ReqT, RespT] = {
    val idx = UnsignedInts.remainder(chId.getAndIncrement, channels.length)
    val ch = channels(idx)
    logger.debug(s"using channel $idx ${ch.getClass.getCanonicalName}")
    ch.newCall(m, o)
  }

  override val authority: String = channels(0).authority

  override def shutdown(): ManagedChannel = {
    channels.foreach(_.shutdown())
    this
  }

  override def isShutdown: Boolean = channels.forall(_.isShutdown)

  override def isTerminated: Boolean = channels.forall(_.isTerminated)

  override def shutdownNow(): ManagedChannel = {
    channels.foreach(_.shutdownNow())
    this
  }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    channels.map(_.awaitTermination(timeout, unit)).forall(_ == true)
  }
}
