package com.google.cloud.imf.gzos

import java.nio.charset.Charset

import com.google.cloud.gszutil.Transcoder

case object Ebcdic extends Transcoder {
  final val charset: Charset = new EBCDIC1()
}
