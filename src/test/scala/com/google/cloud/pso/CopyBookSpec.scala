package com.google.cloud.pso

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.ZReader.ZIterator
import com.google.cloud.gszutil.io.ByteArrayRecordReader
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
    def records = new ZIterator(new ByteArrayRecordReader(Util.readB("test.bin"), copyBook.lRecl, copyBook.lRecl*10))
    assert(records.length == 17)
    val rows = copyBook.reader.readA(records).toArray
    System.out.println(rows.map(_.toString).mkString("\n"))
  }
}
