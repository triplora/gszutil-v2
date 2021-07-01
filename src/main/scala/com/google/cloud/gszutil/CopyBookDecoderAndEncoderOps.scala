package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.Decimal64Decoder
import com.google.cloud.gszutil.Encoding.DecimalToBinaryEncoder

import java.nio.charset.Charset

object CopyBookDecoderAndEncoderOps {

  val charRegex = """PIC X\((\d{1,3})\)""".r
  val charRegex2 = """PIC T\((\d{1,4})\)""".r
  val bytesRegex = """PIC X\((\d{4,})\)""".r
  val numStrRegex = """PIC 9\((\d{1,3})\)""".r
  val intRegex = """PIC S9\((\d{1,3})\) COMP""".r
  val uintRegex = """PIC 9\((\d{1,3})\) COMP""".r
  val decRegex = """PIC S9\((\d{1,3})\) COMP-3""".r
  val decRegex2 = """PIC S9\((\d{1,3})\)V9\((\d{1,3})\) COMP-3""".r
  val decRegex3 = """PIC S9\((\d{1,3})\)V(9{1,6}) COMP-3""".r

  val types: Map[String,(Decoder, BinaryEncoder)] = Map(
    "PIC S9(6)V99 COMP-3" -> (Decimal64Decoder(9,2), DecimalToBinaryEncoder(9,2)),
    "PIC S9(13)V99 COMP-3" -> (Decimal64Decoder(9,2), DecimalToBinaryEncoder(9,2)),
    "PIC S9(7)V99 COMP-3" -> (Decimal64Decoder(7,2), DecimalToBinaryEncoder(7,2)),
  )

  def getPicTCharset: Charset = Charset.forName(aliasToCharset(sys.env.getOrElse("PIC_T_CHARSET", "UTF-8")))

  private def aliasToCharset(alias: String): String = {
    Option(alias).getOrElse("").trim.toLowerCase match {
      case "jpnebcdic1399_4ij" => "x-IBM939"
      case "schebcdic935_6ij" => "x-IBM935"
      case s => s
    }
  }
}
