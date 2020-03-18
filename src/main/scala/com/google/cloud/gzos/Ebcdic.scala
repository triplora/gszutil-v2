package com.google.cloud.gzos

import java.nio.charset.Charset

import com.google.cloud.gszutil.Transcoder

object Ebcdic extends Transcoder {
  final val charset: Charset = new EBCDIC1()
}
