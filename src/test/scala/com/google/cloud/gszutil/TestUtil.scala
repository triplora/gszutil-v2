package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import com.google.common.io.Resources

object TestUtil {
  def getBytes(name: String): ByteBuffer = {
    ByteBuffer.wrap(Resources.toByteArray(Resources.getResource(name)))
  }

  def getData(name: String,
              srcCharset: Charset = StandardCharsets.UTF_8,
              destCharset: Charset = StandardCharsets.UTF_8): ByteBuffer = {
    val example = Resources.toString(Resources.getResource(name), srcCharset)
    ByteBuffer.wrap(example.filterNot(_ == '\n').getBytes(destCharset))
  }
}
