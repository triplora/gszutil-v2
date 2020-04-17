package com.google.cloud.imf.grecv.grpc

import java.io.{InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import io.grpc.Codec

class GzipCodec extends Codec {
  override def getMessageEncoding: String = "gzip"
  override def decompress(is: InputStream): InputStream = new GZIPInputStream(is,4096)
  override def compress(os: OutputStream): OutputStream = new GZIPOutputStream(os, 4096)
}
