package com.google.cloud.imf.grecv.grpc

import java.util.concurrent.atomic.AtomicInteger

import com.google.cloud.imf.util.Logging
import com.google.common.primitives.UnsignedInts
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{CallOptions, Channel, ClientCall, MethodDescriptor}

// z/OS sockets are limited to ~10MB/s so we use multiple
class MultiChannel(b: NettyChannelBuilder, nConnections: Int)
  extends Channel with Logging {
  private val channels: Array[Channel] =
    (0 until nConnections).map(_ => b.build()).toArray

  private val chId = new AtomicInteger

  def newCall[ReqT, RespT](m: MethodDescriptor[ReqT, RespT], o: CallOptions): ClientCall[ReqT, RespT] = {
    val idx = UnsignedInts.remainder(chId.getAndIncrement, channels.length)
    val ch = channels(idx)
    logger.debug(s"using channel $idx ${ch.getClass.getCanonicalName}")
    ch.newCall(m, o)
  }

  override val authority: String = channels(0).authority
}
