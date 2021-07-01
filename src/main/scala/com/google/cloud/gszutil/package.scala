package com.google.cloud

import java.nio.charset.Charset

package object gszutil {

  def getPicTCharset: Charset = Charset.forName(aliasToCharset(sys.env.getOrElse("PIC_T_CHARSET", "UTF-8")))

  private def aliasToCharset(alias: String): String = {
    Option(alias).getOrElse("").trim.toLowerCase match {
      case "jpnebcdic1399_4ij" => "x-IBM939"
      case "schebcdic935_6ij" => "x-IBM935"
      case s => s
    }
  }

}
