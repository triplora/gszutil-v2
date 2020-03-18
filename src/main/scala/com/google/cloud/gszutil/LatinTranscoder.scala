package com.google.cloud.gszutil
import java.nio.charset.{Charset, StandardCharsets}

object LatinTranscoder extends Transcoder {
  override val charset: Charset = StandardCharsets.ISO_8859_1
}
