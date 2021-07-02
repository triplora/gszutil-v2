package com.google.cloud.imf.gzos

import com.google.cloud.gszutil.Transcoder

import java.nio.charset.Charset

case class LocalizedTranscoder(localizedCharset: Option[String]) extends Transcoder {
  override final val charset: Charset = localizedCharset
    .map(aliasToCharset)
    .map(c => Charset.forName(c))
    .getOrElse(new EBCDIC1)
  override val SP: Byte = charset.encode(" ").array().head

  private def aliasToCharset(alias: String): String = {
    Option(alias).getOrElse("").trim.toLowerCase match {
      case "jpnebcdic1399_4ij" => "x-IBM939"
      case "schebcdic935_6ij" => "x-IBM935"
      case s => s
    }
  }
}
