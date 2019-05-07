package com.google.cloud.pso

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.{TestRecordReader, Util}
import com.google.cloud.gszutil.ZReader.ByteIterator
import org.scalatest.FlatSpec

class CopyBookSpec extends FlatSpec {
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
    val reader = copyBook.reader
    val bytes = ByteIterator(new TestRecordReader(Util.readB("test.bin"), copyBook.lRecl, copyBook.lRecl*10))
    val rows = reader.readInternal(bytes).toArray
    assert(rows.nonEmpty)
    System.out.println(rows.map(_.toString).mkString("\n"))
  }
}
