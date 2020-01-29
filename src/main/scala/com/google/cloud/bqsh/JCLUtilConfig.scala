package com.google.cloud.bqsh

case class JCLUtilConfig(src: String = "",
                         dest: String = "",
                         transform: String = "",
                         expressions: Seq[String] = Seq.empty,
                         filter: String = "TD",
                         limit: Int = 4096) {
  def exprs: Seq[(String,String)] = expressions.flatMap(parseExpr)

  def parseExpr(s: String): Option[(String,String)] = {
    if (s.length >= 5 && s.charAt(0) == 's') {
      val a = s.split(s.charAt(1))
      if (a.length == 3) Option((a(1),a(2)))
      else None
    } else None
  }
}
