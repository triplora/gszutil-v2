package com.google.cloud.gszutil

import java.time.format.DateTimeFormatter

trait DateDecoder extends Decoder {
  val format: String
  protected val pattern: String = format.replaceAllLiterally("D","d").replaceAllLiterally("Y","y")
  protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
}
