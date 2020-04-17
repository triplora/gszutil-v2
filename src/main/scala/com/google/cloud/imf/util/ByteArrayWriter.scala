package com.google.cloud.imf.util

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

class ByteArrayWriter(val os: ByteArrayOutputStream = new ByteArrayOutputStream())
  extends PrintStream(os) {
  def result: String = new String(os.toByteArray, StandardCharsets.UTF_8)
}
