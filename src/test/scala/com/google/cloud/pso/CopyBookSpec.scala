package com.google.cloud.pso

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZDataSet, ZIterator}
import org.scalatest.FlatSpec

class CopyBookSpec extends FlatSpec with Logging {

  "CopyBook" should "parse" in {
    for (name <- (1 to 3).map(i => s"test$i.cpy")){
      val cb1 = CopyBook(Util.readS(name))
      val s = cb1.lines.map(_.toString).mkString("\n")
      System.out.println("\n*********************************\n")
      System.out.println(s)
      System.out.println("\n*********************************\n")
    }
  }

  it should "read" in {
    val copyBook = CopyBook(Util.readS("test1.cpy"))
    val reader = new ZDataSet(Util.readB("test.bin"), copyBook.lRecl, copyBook.lRecl*10)
    val (data,offset) = ZIterator(reader)
    val offset1 = offset.toArray
    var n = 0
    val rows = copyBook.reader.readA(data, offset1.iterator)
        .foldLeft(new StringBuilder){(a,b) =>
          n += 1
          a.append(b.toString)
            .append("\n")
        }
    System.out.println(rows)
    assert(n == 17)
  }
}
