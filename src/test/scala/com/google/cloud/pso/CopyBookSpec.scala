package com.google.cloud.pso

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import org.scalatest.FlatSpec

class CopyBookSpec extends FlatSpec with Logging {
  "CopyBook" should "parse" in {
    for (name <- (1 to 3).map(i => s"test$i.cpy")){
      val cb1 = CopyBook(Util.readS(name))
      val s = cb1.Fields.map(_.toString).mkString("\n")
      System.out.println("\n*********************************\n")
      System.out.println(s)
      System.out.println("\n*********************************\n")
    }
  }
}
