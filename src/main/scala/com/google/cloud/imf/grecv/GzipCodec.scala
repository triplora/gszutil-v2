package com.google.cloud.imf.grecv

import java.io.{InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import io.grpc.{Codec, CompressorRegistry}

object GzipCodec extends Codec {
  override def getMessageEncoding: String = "gzip"
  override def decompress(is: InputStream): InputStream = new GZIPInputStream(is,4096)
  override def compress(os: OutputStream): OutputStream = new GZIPOutputStream(os, 4096)
  val compressorRegistry: CompressorRegistry = {
    val cr = CompressorRegistry.newEmptyInstance
    cr.register(GzipCodec)
    cr
  }
}
